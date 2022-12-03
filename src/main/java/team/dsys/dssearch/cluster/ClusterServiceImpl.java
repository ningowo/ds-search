package team.dsys.dssearch.cluster;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.report.RaftNodeReport;
import team.dsys.dssearch.cluster.config.ClusterServiceConfig;
import team.dsys.dssearch.cluster.exception.ClusterServerException;
import team.dsys.dssearch.cluster.lifecycle.TerminationAware;
import team.dsys.dssearch.cluster.module.ClusterServiceModule;
import team.dsys.dssearch.cluster.raft.report.RaftNodeReportSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.inject.name.Names.named;
import static team.dsys.dssearch.cluster.module.ClusterServiceModule.RAFT_NODE_SUPPLIER_KEY;

public class ClusterServiceImpl implements ClusterService {
    private final ClusterServiceConfig config;
    private final RaftEndpoint nodeEndpoint;
    private final Injector injector;
    private final LifecycleManager lifecycleManager;
    private final RaftNode raftNode;
    private final Supplier<RaftNodeReport> raftNodeReportSupplier;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.LATENT);
    private volatile boolean terminationCompleted;

    public ClusterServiceImpl(ClusterServiceConfig config, RaftEndpoint nodeEndpoint, List<RaftEndpoint> initialEndpoints,
                         Map<RaftEndpoint, String> endpointAddresses) {
        try {
            this.config = config;
            this.nodeEndpoint = nodeEndpoint;
            Module module = new ClusterServiceModule(config, nodeEndpoint, initialEndpoints, endpointAddresses);
            this.injector = LifecycleInjector.builder().withModules(module).build().createInjector();
            this.lifecycleManager = injector.getInstance(LifecycleManager.class);

            lifecycleManager.start();
            status.set(Status.RUNNING);

            Supplier<RaftNode> raftNodeSupplier = injector.getInstance(Key.get(new TypeLiteral<Supplier<RaftNode>>() {
            }, named(RAFT_NODE_SUPPLIER_KEY)));
            this.raftNode = raftNodeSupplier.get();
            this.raftNodeReportSupplier = injector.getInstance(RaftNodeReportSupplier.class);


            registerShutdownHook();
        } catch (Throwable t) {
            shutdown();
            throw new ClusterServerException("Could not start server!", t);
        }
    }


    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!isShutdown()) {
                System.out.println(nodeEndpoint.getId() + " shutting down...");
            }

            shutdown();
        }));
    }

    @Nonnull
    @Override
    public ClusterServiceConfig getConfig() {
        return config;
    }

    @Nonnull
    @Override
    public RaftEndpoint getNodeEndpoint() {
        return nodeEndpoint;
    }

    @Nonnull
    @Override
    public RaftNodeReport getRaftNodeReport() {
        return raftNodeReportSupplier.get();
    }

    @Nonnull
    @Override
    public RaftNode getRaftNode() {
        return raftNode;
    }

    @Override
    public void shutdown() {
        if (status.compareAndSet(Status.RUNNING, Status.SHUTTING_DOWN)) {
            try {
                lifecycleManager.close();
            } finally {
                status.set(Status.SHUT_DOWN);
            }
        } else {
            status.compareAndSet(Status.LATENT, Status.SHUT_DOWN);
        }
    }

    @Override
    public boolean isShutdown() {
        return status.get() == Status.SHUT_DOWN;
    }

    @Override
    public void awaitTermination() {
        if (terminationCompleted) {
            return;
        }

        injector.getAllBindings().values().stream()
                .filter(binding -> binding.getProvider().get() instanceof TerminationAware)
                .map(binding -> (TerminationAware) binding.getProvider().get())
                .forEach(TerminationAware::awaitTermination);
        terminationCompleted = true;
    }


    private enum Status {
        LATENT, RUNNING, SHUTTING_DOWN, SHUT_DOWN
    }






}
