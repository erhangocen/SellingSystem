using System.Net.Sockets;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;

public struct RabbitMQPersistentConnection : IDisposable
{
    private readonly IConnectionFactory connectionFactory;
    private readonly int _retryCount;
    private IConnection connection;
    private bool _disposed;
    
    private object lock_object = new object();

    public bool IsConnected => connection != null && connection.IsOpen;

    public RabbitMQPersistentConnection(IConnectionFactory factory, int retryCount = 5)
    {
        connectionFactory = factory;
        _retryCount = retryCount;
    }

    public IModel CreateChannel()
    {
        return connection.CreateModel();
    }
    
    public void Dispose()
    { 
        _disposed = true;
        connection?.Dispose();
    }
    
    private void setConnection(IConnection connection)
    {
        this.connection = connection;
    }

    public bool TryConnect()
    {
        lock (lock_object)
        {
            var policy = Policy.Handle<SocketException>().Or<BrokerUnreachableException>()
                .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, span) => {});


            var factory = connectionFactory;
            var setConnectionD = setConnection;
            policy.Execute(() =>
            {
                setConnectionD(factory.CreateConnection()) ;
            });

            if (IsConnected)
            {
                connection.ConnectionShutdown += Connection_ConnectionShotDown;
                connection.CallbackException += Connection_ConnectionCallBackException;
                connection.ConnectionBlocked += Connection_ConnectionBlocked;
                return true;
            }

            return false;
        }
    }

    private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
    {
        if (_disposed) return;
        TryConnect();
    }

    private void Connection_ConnectionCallBackException(object? sender, CallbackExceptionEventArgs e)
    {
        if (_disposed) return;
        TryConnect();
    }

    private void Connection_ConnectionShotDown(object? sender, ShutdownEventArgs e)
    {
        if (_disposed) return;
        TryConnect();
    }
}