package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/authzed/grpcutil"
	"github.com/cespare/xxhash/v2"
	"github.com/ecordell/optgen/helpers"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"github.com/sean-/sysexits"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // enable gzip compression on all derivative servers

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/dashboard"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch"
	clusterdispatch "github.com/authzed/spicedb/internal/dispatch/cluster"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/gateway"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/services"
	dispatchSvc "github.com/authzed/spicedb/internal/services/dispatch"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/balancer"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ConsistentHashringBuilder is a balancer Builder that uses xxhash as the
// underlying hash for the ConsistentHashringBalancers it creates.
var ConsistentHashringBuilder = balancer.NewConsistentHashringBuilder(
	xxhash.Sum64,
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	// API config
	GRPCServer             util.GRPCServerConfig `debugmap:"visible"`
	GRPCAuthFunc           grpc_auth.AuthFunc    `debugmap:"visible"`
	PresharedSecureKey     []string              `debugmap:"sensitive"`
	ShutdownGracePeriod    time.Duration         `debugmap:"visible"`
	DisableVersionResponse bool                  `debugmap:"visible"`

	// GRPC Gateway config
	HTTPGateway                    util.HTTPServerConfig `debugmap:"visible"`
	HTTPGatewayUpstreamAddr        string                `debugmap:"visible"`
	HTTPGatewayUpstreamTLSCertPath string                `debugmap:"visible"`
	HTTPGatewayCorsEnabled         bool                  `debugmap:"visible"`
	HTTPGatewayCorsAllowedOrigins  []string              `debugmap:"visible-format"`

	// Datastore
	DatastoreConfig datastorecfg.Config `debugmap:"visible"`
	Datastore       datastore.Datastore `debugmap:"visible"`

	// Datastore usage
	MaxCaveatContextSize       int `debugmap:"visible"`
	MaxRelationshipContextSize int `debugmap:"visible"`

	// Namespace cache
	NamespaceCacheConfig CacheConfig `debugmap:"visible"`

	// Schema options
	SchemaPrefixesRequired bool `debugmap:"visible"`

	// Dispatch options
	DispatchServer                    util.GRPCServerConfig   `debugmap:"visible"`
	DispatchMaxDepth                  uint32                  `debugmap:"visible"`
	GlobalDispatchConcurrencyLimit    uint16                  `debugmap:"visible"`
	DispatchConcurrencyLimits         graph.ConcurrencyLimits `debugmap:"visible"`
	DispatchUpstreamAddr              string                  `debugmap:"visible"`
	DispatchUpstreamCAPath            string                  `debugmap:"visible"`
	DispatchUpstreamTimeout           time.Duration           `debugmap:"visible"`
	DispatchClientMetricsEnabled      bool                    `debugmap:"visible"`
	DispatchClientMetricsPrefix       string                  `debugmap:"visible"`
	DispatchClusterMetricsEnabled     bool                    `debugmap:"visible"`
	DispatchClusterMetricsPrefix      string                  `debugmap:"visible"`
	Dispatcher                        dispatch.Dispatcher     `debugmap:"visible"`
	DispatchHashringReplicationFactor uint16                  `debugmap:"visible"`
	DispatchHashringSpread            uint8                   `debugmap:"visible"`

	DispatchCacheConfig        CacheConfig `debugmap:"visible"`
	ClusterDispatchCacheConfig CacheConfig `debugmap:"visible"`

	// API Behavior
	DisableV1SchemaAPI       bool          `debugmap:"visible"`
	V1SchemaAdditiveOnly     bool          `debugmap:"visible"`
	MaximumUpdatesPerWrite   uint16        `debugmap:"visible"`
	MaximumPreconditionCount uint16        `debugmap:"visible"`
	MaxDatastoreReadPageSize uint64        `debugmap:"visible"`
	StreamingAPITimeout      time.Duration `debugmap:"visible"`

	// Additional Services
	DashboardAPI util.HTTPServerConfig `debugmap:"visible"`
	MetricsAPI   util.HTTPServerConfig `debugmap:"visible"`

	// Middleware for grpc API
	MiddlewareModification []MiddlewareModification `debugmap:"hidden"`

	// Middleware for internal dispatch API
	DispatchUnaryMiddleware     []grpc.UnaryServerInterceptor  `debugmap:"hidden"`
	DispatchStreamingMiddleware []grpc.StreamServerInterceptor `debugmap:"hidden"`

	// Telemetry
	SilentlyDisableTelemetry bool          `debugmap:"visible"`
	TelemetryCAOverridePath  string        `debugmap:"visible"`
	TelemetryEndpoint        string        `debugmap:"visible"`
	TelemetryInterval        time.Duration `debugmap:"visible"`
}

type closeableStack struct {
	closers []func() error
}

func (c *closeableStack) AddWithError(closer func() error) {
	c.closers = append(c.closers, closer)
}

func (c *closeableStack) AddCloser(closer io.Closer) {
	if closer != nil {
		c.closers = append(c.closers, closer.Close)
	}
}

func (c *closeableStack) AddWithoutError(closer func()) {
	c.closers = append(c.closers, func() error {
		closer()
		return nil
	})
}

func (c *closeableStack) Close() error {
	var err error
	// closer in reverse order how it's expected in deferred funcs
	for i := len(c.closers) - 1; i >= 0; i-- {
		if closerErr := c.closers[i](); closerErr != nil {
			err = multierror.Append(err, closerErr)
		}
	}
	return err
}

func (c *closeableStack) CloseIfError(err error) error {
	if err != nil {
		return c.Close()
	}
	return nil
}

// Complete validates the config and fills out defaults.
// if there is no error, a completedServerConfig (with limited options for
// mutation) is returned.
func (c *Config) Complete(ctx context.Context) (RunnableServer, error) {
	log.Ctx(ctx).Info().Fields(helpers.Flatten(c.DebugMap())).Msg("configuration")

	closeables := closeableStack{}
	var err error
	defer func() {
		// if an error happens during the execution of Complete, all resources are cleaned up
		if closeableErr := closeables.CloseIfError(err); closeableErr != nil {
			log.Ctx(ctx).Err(closeableErr).Msg("failed to clean up resources on Config.Complete")
		}
	}()

	if len(c.PresharedSecureKey) < 1 && c.GRPCAuthFunc == nil {
		return nil, fmt.Errorf("a preshared key must be provided to authenticate API requests")
	}

	if c.GRPCAuthFunc == nil {
		log.Ctx(ctx).Trace().Int("preshared-keys-count", len(c.PresharedSecureKey)).Msg("using gRPC auth with preshared key(s)")
		for index, presharedKey := range c.PresharedSecureKey {
			if len(presharedKey) == 0 {
				return nil, fmt.Errorf("preshared key #%d is empty", index+1)
			}

			log.Ctx(ctx).Trace().Int("preshared-key-"+strconv.Itoa(index+1)+"-length", len(presharedKey)).Msg("preshared key configured")
		}

		c.GRPCAuthFunc = auth.MustRequirePresharedKey(c.PresharedSecureKey)
	} else {
		log.Ctx(ctx).Trace().Msg("using preconfigured auth function")
	}

	ds := c.Datastore
	if ds == nil {
		var err error
		ds, err = datastorecfg.NewDatastore(context.Background(), c.DatastoreConfig.ToOption())
		if err != nil {
			return nil, spiceerrors.NewTerminationErrorBuilder(fmt.Errorf("failed to create datastore: %w", err)).
				Component("datastore").
				ExitCode(sysexits.Config).
				Error()
		}
	}
	closeables.AddWithError(ds.Close)

	nscc, err := c.NamespaceCacheConfig.Complete()
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace cache: %w", err)
	}
	log.Ctx(ctx).Info().EmbedObject(nscc).Msg("configured namespace cache")

	ds = proxy.NewCachingDatastoreProxy(ds, nscc)
	ds = proxy.NewObservableDatastoreProxy(ds)
	closeables.AddWithError(ds.Close)

	enableGRPCHistogram()

	dispatcher := c.Dispatcher
	if dispatcher == nil {
		cc, err := c.DispatchCacheConfig.WithRevisionParameters(
			c.DatastoreConfig.RevisionQuantization,
			c.DatastoreConfig.FollowerReadDelay,
			c.DatastoreConfig.MaxRevisionStalenessPercent,
		).Complete()
		if err != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", err)
		}
		closeables.AddWithoutError(cc.Close)
		log.Ctx(ctx).Info().EmbedObject(cc).Msg("configured dispatch cache")

		dispatchPresharedKey := ""
		if len(c.PresharedSecureKey) > 0 {
			dispatchPresharedKey = c.PresharedSecureKey[0]
		}

		specificConcurrencyLimits := c.DispatchConcurrencyLimits
		concurrencyLimits := specificConcurrencyLimits.WithOverallDefaultLimit(c.GlobalDispatchConcurrencyLimit)

		hashringConfigJSON, err := (&balancer.ConsistentHashringBalancerConfig{
			ReplicationFactor: c.DispatchHashringReplicationFactor,
			Spread:            c.DispatchHashringSpread,
		}).ToServiceConfigJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to create hashring balancer config: %w", err)
		}

		dispatcher, err = combineddispatch.NewDispatcher(
			combineddispatch.UpstreamAddr(c.DispatchUpstreamAddr),
			combineddispatch.UpstreamCAPath(c.DispatchUpstreamCAPath),
			combineddispatch.GrpcPresharedKey(dispatchPresharedKey),
			combineddispatch.GrpcDialOpts(
				grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
				grpc.WithDefaultServiceConfig(hashringConfigJSON),
			),
			combineddispatch.MetricsEnabled(c.DispatchClientMetricsEnabled),
			combineddispatch.PrometheusSubsystem(c.DispatchClientMetricsPrefix),
			combineddispatch.Cache(cc),
			combineddispatch.ConcurrencyLimits(concurrencyLimits),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", err)
		}

		log.Ctx(ctx).Info().EmbedObject(concurrencyLimits).RawJSON("balancerconfig", []byte(hashringConfigJSON)).Msg("configured dispatcher")
	}
	closeables.AddWithError(dispatcher.Close)

	if len(c.DispatchUnaryMiddleware) == 0 && len(c.DispatchStreamingMiddleware) == 0 {
		if c.GRPCAuthFunc == nil {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, auth.MustRequirePresharedKey(c.PresharedSecureKey), ds)
		} else {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, c.GRPCAuthFunc, ds)
		}
	}

	var cachingClusterDispatch dispatch.Dispatcher
	if c.DispatchServer.Enabled {
		cdcc, err := c.ClusterDispatchCacheConfig.WithRevisionParameters(
			c.DatastoreConfig.RevisionQuantization,
			c.DatastoreConfig.FollowerReadDelay,
			c.DatastoreConfig.MaxRevisionStalenessPercent,
		).Complete()
		if err != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
		}
		log.Ctx(ctx).Info().EmbedObject(cdcc).Msg("configured cluster dispatch cache")
		closeables.AddWithoutError(cdcc.Close)

		cachingClusterDispatch, err = clusterdispatch.NewClusterDispatcher(
			dispatcher,
			clusterdispatch.MetricsEnabled(c.DispatchClusterMetricsEnabled),
			clusterdispatch.PrometheusSubsystem(c.DispatchClusterMetricsPrefix),
			clusterdispatch.Cache(cdcc),
			clusterdispatch.RemoteDispatchTimeout(c.DispatchUpstreamTimeout),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
		}
		closeables.AddWithError(cachingClusterDispatch.Close)
	}

	dispatchGrpcServer, err := c.DispatchServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			dispatchSvc.RegisterGrpcServices(server, cachingClusterDispatch)
		},
		grpc.ChainUnaryInterceptor(c.DispatchUnaryMiddleware...),
		grpc.ChainStreamInterceptor(c.DispatchStreamingMiddleware...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatch gRPC server: %w", err)
	}
	closeables.AddWithoutError(dispatchGrpcServer.GracefulStop)

	datastoreFeatures, err := ds.Features(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining datastore features: %w", err)
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if c.DisableV1SchemaAPI {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	} else if c.V1SchemaAdditiveOnly {
		v1SchemaServiceOption = services.V1SchemaServiceAdditiveOnly
	}

	watchServiceOption := services.WatchServiceEnabled
	if !datastoreFeatures.Watch.Enabled {
		log.Ctx(ctx).Warn().Str("reason", datastoreFeatures.Watch.Reason).Msg("watch api disabled; underlying datastore does not support it")
		watchServiceOption = services.WatchServiceDisabled
	}

	defaultMiddlewareChain, err := DefaultMiddleware(log.Logger, c.GRPCAuthFunc, !c.DisableVersionResponse, dispatcher, ds)
	if err != nil {
		return nil, fmt.Errorf("error building default middleware: %w", err)
	}

	unaryMiddleware, streamingMiddleware, err := c.buildMiddleware(defaultMiddlewareChain)
	if err != nil {
		return nil, fmt.Errorf("error building Middlewares: %w", err)
	}

	permSysConfig := v1svc.PermissionsServerConfig{
		MaxPreconditionsCount:      c.MaximumPreconditionCount,
		MaxUpdatesPerWrite:         c.MaximumUpdatesPerWrite,
		MaximumAPIDepth:            c.DispatchMaxDepth,
		MaxCaveatContextSize:       c.MaxCaveatContextSize,
		MaxRelationshipContextSize: c.MaxRelationshipContextSize,
		MaxDatastoreReadPageSize:   c.MaxDatastoreReadPageSize,
		StreamingAPITimeout:        c.StreamingAPITimeout,
	}

	healthManager := health.NewHealthManager(dispatcher, ds)
	grpcServer, err := c.GRPCServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			services.RegisterGrpcServices(
				server,
				healthManager,
				dispatcher,
				v1SchemaServiceOption,
				watchServiceOption,
				permSysConfig,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC server: %w", err)
	}
	closeables.AddWithoutError(grpcServer.GracefulStop)

	gatewayServer, gatewayCloser, err := c.initializeGateway(ctx)
	if err != nil {
		return nil, err
	}
	closeables.AddCloser(gatewayCloser)
	closeables.AddWithoutError(gatewayServer.Close)

	dashboardServer, err := c.DashboardAPI.Complete(zerolog.InfoLevel, dashboard.NewHandler(
		c.GRPCServer.Address,
		c.GRPCServer.TLSKeyPath != "" || c.GRPCServer.TLSCertPath != "",
		c.DatastoreConfig.Engine,
		ds,
	))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dashboard server: %w", err)
	}
	closeables.AddWithoutError(dashboardServer.Close)

	var telemetryRegistry *prometheus.Registry

	reporter := telemetry.DisabledReporter
	if c.SilentlyDisableTelemetry {
		reporter = telemetry.SilentlyDisabledReporter
	} else if c.TelemetryEndpoint != "" && c.DatastoreConfig.DisableStats {
		reporter = telemetry.DisabledReporter
	} else if c.TelemetryEndpoint != "" {
		log.Ctx(ctx).Debug().Msg("initializing telemetry collector")
		registry, err := telemetry.RegisterTelemetryCollector(c.DatastoreConfig.Engine, ds)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize telemetry collector: %w", err)
		}

		telemetryRegistry = registry
		reporter, err = telemetry.RemoteReporter(
			telemetryRegistry, c.TelemetryEndpoint, c.TelemetryCAOverridePath, c.TelemetryInterval,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize metrics reporter: %w", err)
		}
	}

	metricsServer, err := c.MetricsAPI.Complete(zerolog.InfoLevel, MetricsHandler(telemetryRegistry, c))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics server: %w", err)
	}
	closeables.AddWithoutError(metricsServer.Close)

	return &completedServerConfig{
		gRPCServer:          grpcServer,
		dispatchGRPCServer:  dispatchGrpcServer,
		gatewayServer:       gatewayServer,
		metricsServer:       metricsServer,
		dashboardServer:     dashboardServer,
		unaryMiddleware:     unaryMiddleware,
		streamingMiddleware: streamingMiddleware,
		presharedKeys:       c.PresharedSecureKey,
		telemetryReporter:   reporter,
		healthManager:       healthManager,
		closeFunc:           closeables.Close,
	}, nil
}

func (c *Config) buildMiddleware(defaultMiddleware *MiddlewareChain) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor, error) {
	chain := MiddlewareChain{}
	if defaultMiddleware != nil {
		chain.chain = append(chain.chain, defaultMiddleware.chain...)
	}
	if err := chain.modify(c.MiddlewareModification...); err != nil {
		return nil, nil, err
	}
	unaryOutput, streamingOutput := chain.ToGRPCInterceptors()
	return unaryOutput, streamingOutput, nil
}

// initializeGateway Configures the gateway to serve HTTP
func (c *Config) initializeGateway(ctx context.Context) (util.RunnableHTTPServer, io.Closer, error) {
	if len(c.HTTPGatewayUpstreamAddr) == 0 {
		c.HTTPGatewayUpstreamAddr = c.GRPCServer.Address
	} else {
		log.Ctx(ctx).Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("Overriding REST gateway upstream")
	}

	if len(c.HTTPGatewayUpstreamTLSCertPath) == 0 {
		c.HTTPGatewayUpstreamTLSCertPath = c.GRPCServer.TLSCertPath
	} else {
		log.Ctx(ctx).Info().Str("cert-path", c.HTTPGatewayUpstreamTLSCertPath).Msg("Overriding REST gateway upstream TLS")
	}

	// If the requested network is a buffered one, then disable the HTTPGateway.
	if c.GRPCServer.Network == util.BufferedNetwork {
		c.HTTPGateway.HTTPEnabled = false
		c.HTTPGatewayUpstreamAddr = "invalidaddr:1234" // We need an address-like value here for gRPC
	}

	var gatewayHandler http.Handler
	closeableGatewayHandler, err := gateway.NewHandler(ctx, c.HTTPGatewayUpstreamAddr, c.HTTPGatewayUpstreamTLSCertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}
	gatewayHandler = closeableGatewayHandler

	if c.HTTPGatewayCorsEnabled {
		log.Ctx(ctx).Info().Strs("origins", c.HTTPGatewayCorsAllowedOrigins).Msg("Setting REST gateway CORS policy")
		gatewayHandler = cors.New(cors.Options{
			AllowedOrigins:   c.HTTPGatewayCorsAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   []string{"Authorization", "Content-Type"},
			Debug:            log.Debug().Enabled(),
		}).Handler(gatewayHandler)
	}

	if c.HTTPGateway.HTTPEnabled {
		log.Ctx(ctx).Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("starting REST gateway")
	}

	gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, gatewayHandler)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}
	return gatewayServer, closeableGatewayHandler, nil
}

// RunnableServer is a spicedb service set ready to run
type RunnableServer interface {
	Run(ctx context.Context) error
	GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error)
}

// completedServerConfig holds the full configuration to run a spicedb server,
// but is assumed have already been validated via `Complete()` on Config.
// It offers limited options for mutation before Run() starts the services.
type completedServerConfig struct {
	gRPCServer         util.RunnableGRPCServer
	dispatchGRPCServer util.RunnableGRPCServer
	gatewayServer      util.RunnableHTTPServer
	metricsServer      util.RunnableHTTPServer
	dashboardServer    util.RunnableHTTPServer
	telemetryReporter  telemetry.Reporter
	healthManager      health.Manager

	unaryMiddleware     []grpc.UnaryServerInterceptor
	streamingMiddleware []grpc.StreamServerInterceptor
	presharedKeys       []string
	closeFunc           func() error
}

func (c *completedServerConfig) GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if len(c.presharedKeys) == 0 {
		return c.gRPCServer.DialContext(ctx, opts...)
	}
	if c.gRPCServer.Insecure() {
		opts = append(opts, grpcutil.WithInsecureBearerToken(c.presharedKeys[0]))
	} else {
		opts = append(opts, grpcutil.WithBearerToken(c.presharedKeys[0]))
	}
	return c.gRPCServer.DialContext(ctx, opts...)
}

func (c *completedServerConfig) DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error) {
	return c.dispatchGRPCServer.NetDialContext(ctx, s)
}

func (c *completedServerConfig) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	stopOnCancelWithErr := func(stopFn func() error) func() error {
		return func() error {
			<-ctx.Done()
			return stopFn()
		}
	}

	grpcServer := c.gRPCServer.WithOpts(grpc.ChainUnaryInterceptor(c.unaryMiddleware...), grpc.ChainStreamInterceptor(c.streamingMiddleware...))
	g.Go(c.healthManager.Checker(ctx))
	g.Go(grpcServer.Listen(ctx))
	g.Go(c.dispatchGRPCServer.Listen(ctx))
	g.Go(c.gatewayServer.ListenAndServe)
	g.Go(c.metricsServer.ListenAndServe)
	g.Go(c.dashboardServer.ListenAndServe)
	g.Go(func() error { return c.telemetryReporter(ctx) })

	g.Go(stopOnCancelWithErr(c.closeFunc))

	if err := g.Wait(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("error shutting down server")
		return err
	}

	return nil
}

var promOnce sync.Once

// enableGRPCHistogram enables the standard time history for gRPC requests,
// ensuring that it is only enabled once
func enableGRPCHistogram() {
	// EnableHandlingTimeHistogram is not thread safe and only needs to happen
	// once
	promOnce.Do(func() {
		grpcprom.EnableHandlingTimeHistogram(grpcprom.WithHistogramBuckets(
			[]float64{.006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1.000},
		))
	})
}
