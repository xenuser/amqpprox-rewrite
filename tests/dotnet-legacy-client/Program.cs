// SPDX-License-Identifier: Apache-2.0
//
// Exercises amqpprox with the client library used by the legacy .NET
// application (RabbitMQ.Client 6.8.1) and prints what that client sees for
// typical legacy declarations. Every scenario uses its own connection.
// See README.md in this directory.
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

var options = Options.Parse(args);
if (options is null)
{
    return 2;
}

var prefix = "legacy-client-check-" + Guid.NewGuid().ToString("N")[..8];
var created = new List<string>();
var results = new List<(string Scenario, string Outcome)>();

var library = typeof(ConnectionFactory).Assembly
    .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
    .OfType<System.Reflection.AssemblyInformationalVersionAttribute>()
    .FirstOrDefault()?.InformationalVersion;
Console.WriteLine($"RabbitMQ.Client {library}, " +
                  $"proxy {options.Host}:{options.Port}, vhost '{options.VirtualHost}', run {prefix}");

var defaults = new ConnectionFactory();
Console.WriteLine($"Client defaults: AutomaticRecoveryEnabled={defaults.AutomaticRecoveryEnabled}, " +
                  $"TopologyRecoveryEnabled={defaults.TopologyRecoveryEnabled}, " +
                  $"NetworkRecoveryInterval={defaults.NetworkRecoveryInterval.TotalSeconds}s, " +
                  $"RequestedHeartbeat={defaults.RequestedHeartbeat.TotalSeconds}s");

// How the client maps URIs to vhosts: a trailing slash selects the empty vhost.
foreach (var uri in new[] { $"amqp://{options.Host}:{options.Port}", $"amqp://{options.Host}:{options.Port}/" })
{
    var f = new ConnectionFactory { Uri = new Uri(uri) };
    Console.WriteLine($"URI {uri,-35} -> VirtualHost '{f.VirtualHost}'");
}

Run("durable-named", channel =>
{
    var name = Track(prefix + "-durable");
    var ok = channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false, arguments: null);
    return $"declared '{ok.QueueName}' (expected: converted to quorum)";
});

Run("classic-lazy-args", channel =>
{
    var name = Track(prefix + "-lazy");
    var arguments = new Dictionary<string, object>
    {
        ["x-queue-type"] = "classic",
        ["x-queue-mode"] = "lazy",
        ["x-message-ttl"] = 60000,
    };
    var ok = channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false, arguments: arguments);
    return $"declared '{ok.QueueName}' (expected: quorum, x-queue-mode dropped)";
});

// A named queue that is only non-durable, a common legacy pattern.
Run("transient-named", channel =>
{
    var name = Track(prefix + "-transient");
    var ok = channel.QueueDeclare(name, durable: false, exclusive: false, autoDelete: false, arguments: null);
    return $"declared '{ok.QueueName}' (proxy pass-through or --promoteTransientQueuesToDurable mode)";
});

// QueueDeclare(name) uses the library defaults durable=false, exclusive=true,
// autoDelete=true: a common legacy pattern that cannot become quorum.
Run("library-defaults", channel =>
{
    var ok = channel.QueueDeclare(Track(prefix + "-defaults"));
    return $"declared '{ok.QueueName}' (proxy pass-through mode)";
});

Run("server-named", channel =>
{
    var ok = channel.QueueDeclare();
    return $"declared '{ok.QueueName}' (proxy pass-through mode)";
});

Run("global-qos-consume", channel =>
{
    var name = Track(prefix + "-qos");
    channel.QueueDeclare(name, durable: true, exclusive: false, autoDelete: false, arguments: null);
    channel.BasicQos(0, 10, global: true);
    var tag = channel.BasicConsume(name, autoAck: false, consumer: new EventingBasicConsumer(channel));
    return $"consuming with global QoS, tag '{tag}'";
});

if (options.ExistingQueue is not null)
{
    // Redeclare a queue that already exists on the broker, as the legacy app
    // does at startup. An existing classic queue makes the broker refuse the
    // rewritten quorum declaration.
    Run("existing-queue", channel =>
    {
        var ok = channel.QueueDeclare(options.ExistingQueue, durable: true, exclusive: false, autoDelete: false, arguments: null);
        return $"declared '{ok.QueueName}'";
    });
}

if (options.WatchRecovery > 0)
{
    WatchRecovery();
}

Cleanup();

Console.WriteLine();
Console.WriteLine("Summary");
foreach (var (scenario, outcome) in results)
{
    Console.WriteLine($"  {scenario,-20} {outcome}");
}
return 0;

string Track(string name)
{
    created.Add(name);
    return name;
}

ConnectionFactory Factory(string scenario, bool recovery = false) => new()
{
    HostName = options.Host,
    Port = options.Port,
    VirtualHost = options.VirtualHost,
    UserName = options.User,
    Password = options.Password,
    ClientProvidedName = "legacy-client-check/" + scenario,
    AutomaticRecoveryEnabled = recovery,
    Ssl = new SslOption
    {
        Enabled = options.Tls,
        ServerName = options.ServerName ?? options.Host,
    },
};

void Run(string scenario, Func<IModel, string> body)
{
    Console.WriteLine();
    Console.WriteLine($"== {scenario}");
    string outcome;
    try
    {
        using var connection = Factory(scenario).CreateConnection();
        connection.ConnectionShutdown += (_, e) => Console.WriteLine($"   connection shutdown: {Describe(e)}");
        using var channel = connection.CreateModel();
        channel.ModelShutdown += (_, e) => Console.WriteLine($"   channel shutdown: {Describe(e)}");
        outcome = "OK: " + body(channel);
    }
    catch (OperationInterruptedException e)
    {
        outcome = "REFUSED: " + Describe(e.ShutdownReason);
    }
    catch (Exception e) when (e is BrokerUnreachableException or AlreadyClosedException or IOException)
    {
        outcome = $"FAILED: {e.GetType().Name}: {e.InnerException?.Message ?? e.Message}";
    }
    Console.WriteLine($"   {outcome}");
    results.Add((scenario, outcome));
}

// Reproduces the application's reconnect behaviour: with automatic recovery
// (the library default), a declaration the proxy refuses closes the
// connection, and the library reconnects and recovers the recorded topology.
void WatchRecovery()
{
    Console.WriteLine();
    Console.WriteLine($"== recovery (watching {options.WatchRecovery}s)");
    IConnection connection;
    try
    {
        connection = Factory("recovery", recovery: true).CreateConnection();
    }
    catch (BrokerUnreachableException e)
    {
        results.Add(("recovery", $"FAILED: {e.InnerException?.Message ?? e.Message}"));
        return;
    }
    using var owned = connection;
    var recoverable = (IAutorecoveringConnection)connection;
    var recoveries = 0;
    recoverable.RecoverySucceeded += (_, _) =>
    {
        recoveries++;
        Console.WriteLine($"   {DateTime.Now:HH:mm:ss} recovery succeeded");
    };
    recoverable.ConnectionRecoveryError += (_, e) =>
        Console.WriteLine($"   {DateTime.Now:HH:mm:ss} recovery error: {e.Exception.Message}");
    connection.ConnectionShutdown += (_, e) =>
        Console.WriteLine($"   {DateTime.Now:HH:mm:ss} connection shutdown: {Describe(e)}");

    using var channel = connection.CreateModel();
    channel.QueueDeclare(Track(prefix + "-recovery"), durable: true, exclusive: false, autoDelete: false, arguments: null);
    try
    {
        channel.QueueDeclare(prefix + "-recovery-defaults");
    }
    catch (OperationInterruptedException e)
    {
        Console.WriteLine($"   {DateTime.Now:HH:mm:ss} refused: {Describe(e.ShutdownReason)}");
    }
    Thread.Sleep(TimeSpan.FromSeconds(options.WatchRecovery));
    results.Add(("recovery", $"{recoveries} automatic recoveries in {options.WatchRecovery}s"));
}

void Cleanup()
{
    if (created.Count == 0)
    {
        return;
    }
    try
    {
        using var connection = Factory("cleanup").CreateConnection();
        foreach (var name in created)
        {
            using var channel = connection.CreateModel();
            try
            {
                channel.QueueDelete(name);
            }
            catch (OperationInterruptedException)
            {
                // Not created, e.g. refused or exclusive and already gone.
            }
        }
    }
    catch (Exception e) when (e is BrokerUnreachableException or AlreadyClosedException or OperationInterruptedException)
    {
        Console.WriteLine($"Cleanup incomplete ({e.Message}); delete queues starting with {prefix}");
    }
}

static string Describe(ShutdownEventArgs? e) => e is null
    ? "(no reason)"
    : $"{e.Initiator} {e.ReplyCode} \"{e.ReplyText}\" (class {e.ClassId}, method {e.MethodId})";

sealed record Options(string Host, int Port, string VirtualHost, string User, string Password,
                      bool Tls, string? ServerName, string? ExistingQueue, int WatchRecovery)
{
    public static Options? Parse(string[] args)
    {
        var values = new Dictionary<string, string>();
        var flags = new HashSet<string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--tls")
            {
                flags.Add("tls");
            }
            else if (args[i].StartsWith("--") && i + 1 < args.Length)
            {
                values[args[i][2..]] = args[++i];
            }
            else
            {
                return Usage($"Unexpected argument '{args[i]}'");
            }
        }
        if (!values.TryGetValue("host", out var host))
        {
            return Usage("--host is required");
        }
        var password = Environment.GetEnvironmentVariable("RMQ_PASSWORD");
        if (string.IsNullOrEmpty(password))
        {
            return Usage("Set the password in environment variable RMQ_PASSWORD");
        }
        return new Options(
            host,
            int.Parse(values.GetValueOrDefault("port", "5673")),
            values.GetValueOrDefault("vhost", "/"),
            values.GetValueOrDefault("user", "guest"),
            password,
            flags.Contains("tls"),
            values.GetValueOrDefault("server-name"),
            values.GetValueOrDefault("existing-queue"),
            int.Parse(values.GetValueOrDefault("watch-recovery", "0")));
    }

    static Options? Usage(string error)
    {
        Console.Error.WriteLine(error);
        Console.Error.WriteLine("Usage: dotnet run -- --host PROXY [--port 5673] [--vhost /] [--user NAME] " +
                                "[--tls [--server-name NAME]] [--existing-queue NAME] [--watch-recovery SECONDS]");
        return null;
    }
}
