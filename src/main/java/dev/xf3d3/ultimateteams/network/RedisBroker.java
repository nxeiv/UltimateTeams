package dev.xf3d3.ultimateteams.network;

import dev.xf3d3.ultimateteams.UltimateTeams;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.util.Pool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Redis message broker implementation.
 */
public class RedisBroker extends PluginMessageBroker {
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private volatile Pool<Jedis> jedisPool;
    private volatile JedisPubSub subscriber;
    private volatile Thread subscriberThread;

    public RedisBroker(@NotNull UltimateTeams plugin) {
        super(plugin);
    }

    @Override
    public void initialize() throws RuntimeException {
        super.initialize();

        this.jedisPool = establishJedisPool();
        this.subscriber = createSubscriber();

        Thread worker = new Thread(this::runSubscriber, "UltimateTeams_redis_subscriber");
        worker.setDaemon(true);
        this.subscriberThread = worker;
        worker.start();

        plugin.log(Level.INFO, "Initialized Redis connection pool");
    }

    @NotNull
    private Pool<Jedis> establishJedisPool() {
        return new JedisPool(
                new JedisPoolConfig(),
                plugin.getSettings().getCrossServer().getRedis().getHost(),
                plugin.getSettings().getCrossServer().getRedis().getPort(),
                0,
                plugin.getSettings().getCrossServer().getRedis().getPassword().isEmpty()
                        ? null
                        : plugin.getSettings().getCrossServer().getRedis().getPassword(),
                plugin.getSettings().getCrossServer().getRedis().isUseSsl()
        );
    }

    @NotNull
    private JedisPubSub createSubscriber() {
        return new JedisPubSub() {
            @Override
            public void onMessage(@NotNull String channel, @NotNull String encodedMessage) {
                if (closing.get() || !channel.equals(getSubChannelId())) {
                    return;
                }

                try {
                    Message message = plugin.getMessageFromJson(encodedMessage);
                    // Redis invokes this callback on its subscriber thread. Marshal
                    // Bukkit/player access and broker dispatch onto the server scheduler.
                    plugin.runSync(task -> {
                        if (closing.get()) {
                            return;
                        }

                        try {
                            if (message.getTargetType() == Message.TargetType.PLAYER) {
                                Bukkit.getOnlinePlayers().stream()
                                        .filter(player -> player.getName().equalsIgnoreCase(message.getTarget()))
                                        .findFirst()
                                        .ifPresent(receiver -> handle(receiver, message));
                                return;
                            }

                            handle(plugin.getUsersStorageUtil().getOnlineUsers().stream().findAny().orElse(null), message);
                        } catch (RuntimeException exception) {
                            plugin.getLogger().log(Level.WARNING, "Could not process a Redis team message on the server scheduler.", exception);
                        }
                    });
                } catch (RuntimeException exception) {
                    plugin.getLogger().log(Level.WARNING, "Ignoring an invalid Redis team message.", exception);
                }
            }
        };
    }

    private void runSubscriber() {
        Pool<Jedis> pool = jedisPool;
        JedisPubSub activeSubscriber = subscriber;
        if (closing.get() || pool == null || activeSubscriber == null) {
            return;
        }

        try (Jedis jedis = pool.getResource()) {
            if (!closing.get()) {
                jedis.subscribe(activeSubscriber, getSubChannelId());
            }
        } catch (Exception exception) {
            if (!closing.get()) {
                plugin.getLogger().log(Level.SEVERE, "The UltimateTeams Redis subscriber stopped unexpectedly.", exception);
            }
        }
    }

    @Override
    protected void send(@NotNull Message message, @Nullable Player sender) {
        if (closing.get()) {
            return;
        }

        plugin.runAsync(task -> {
            Pool<Jedis> pool = jedisPool;
            if (closing.get() || pool == null) {
                return;
            }

            try (Jedis jedis = pool.getResource()) {
                if (!closing.get()) {
                    jedis.publish(getSubChannelId(), plugin.getGson().toJson(message));
                }
            } catch (Exception exception) {
                if (!closing.get()) {
                    plugin.getLogger().log(Level.WARNING, "Failed to publish an UltimateTeams Redis message.", exception);
                }
            }
        });
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }

        super.close();

        JedisPubSub activeSubscriber = subscriber;
        if (activeSubscriber != null) {
            try {
                activeSubscriber.unsubscribe();
            } catch (RuntimeException exception) {
                plugin.getLogger().log(Level.FINE, "Redis subscriber was already stopping.", exception);
            }
        }

        Thread worker = subscriberThread;
        if (worker != null && worker != Thread.currentThread()) {
            worker.interrupt();
            try {
                worker.join(2000L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        Pool<Jedis> pool = jedisPool;
        if (pool != null) {
            pool.close();
            jedisPool = null;
        }

        subscriber = null;
        subscriberThread = null;
    }
}
