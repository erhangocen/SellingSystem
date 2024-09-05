using System.Net.Http.Json;
using System.Net.Mail;
using System.Net.Sockets;
using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using Polly.CircuitBreaker;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace EventBus.RabbitMQ;

public class EventBusRabbitMQ : BaseEventBus
{
    private RabbitMQPersistentConnection _persistentConnection;
    private IConnectionFactory _connectionFactory;
    private readonly IModel _consumerChannel;
    
    public EventBusRabbitMQ(IServiceProvider serviceProvider, EventBusConfig eventBusConfig) : base(serviceProvider, eventBusConfig)
    {
        SetConnectionFactory(eventBusConfig);
        _persistentConnection = new RabbitMQPersistentConnection(_connectionFactory, eventBusConfig.ConnectionRetryCount);
        _consumerChannel = CreateConsumerChannel();
        SubscriptionManager.OnEventRemoved += SubsManager_OnEventRemoved;
    }

    private void SubsManager_OnEventRemoved(object? sender, string eventName)
    {
        eventName = ProcessEventName(eventName);

        if (!_persistentConnection.IsConnected)
        {
            _persistentConnection.TryConnect();
        }
        _consumerChannel.QueueUnbind(queue:eventName, exchange:EventBusConfig.DefaultTopicName, routingKey:eventName);

        if (SubscriptionManager.IsEmpty)
        {
            _consumerChannel.Close();
        }
    }

    public override void Publish(IntegrationEvent @event)
    {
        if (!_persistentConnection.IsConnected)
        {
            _persistentConnection.TryConnect();
        }
        
        var policy = Policy.Handle<BrokerUnreachableException>().Or<SocketException>().WaitAndRetry(EventBusConfig.ConnectionRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, span) => {});
        
        var eventName = @event.GetType().Name;
        eventName = ProcessEventName(eventName);
        
        _consumerChannel.ExchangeDeclare(exchange:EventBusConfig.DefaultTopicName, type: "direct");
        
        string message = JsonConvert.SerializeObject(@event);
        byte[] messageBody = Encoding.UTF8.GetBytes(message);

        policy.Execute(() =>
        {
            var properties = _consumerChannel.CreateBasicProperties();
            properties.DeliveryMode = 2; // Persistent
            
            _consumerChannel.QueueDeclare(queue: GetSubName(eventName),
                durable:true, exclusive:false, autoDelete: false, arguments: null
                );
            
            _consumerChannel.BasicPublish(exchange:EventBusConfig.DefaultTopicName, routingKey:eventName, mandatory:true, basicProperties:properties, body:messageBody);
            
            
        });
    }

    public override void Subscribe<T, TH>()
    {
        string eventName = typeof(T).Name;
        eventName = ProcessEventName(eventName);
        
        bool haveSubscriptionEvent = SubscriptionManager.HasSubscriptionsForEvent(eventName);
        if (!haveSubscriptionEvent)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }
            _consumerChannel.QueueDeclare(queue: GetSubName(eventName), durable:true, exclusive:false, autoDelete: false, arguments: null);
            
            _consumerChannel.QueueBind(queue:GetSubName(eventName), exchange:EventBusConfig.DefaultTopicName, routingKey:eventName);
        }
        SubscriptionManager.AddSubscription<T, TH>();
        StartBasicConsume(eventName);
    }

    public override void UnSubscribe<T, TH>()
    {
        SubscriptionManager.RemoveSubscription<T, TH>();
    }

    private IModel CreateConsumerChannel()
    {
        if (!_persistentConnection.IsConnected)
        {
            _persistentConnection.TryConnect();
        }

        var channel = _persistentConnection.CreateChannel();
        channel.ExchangeDeclare(exchange:EventBusConfig?.DefaultTopicName, type:"direct");

        return channel;
    }

    private void SetConnectionFactory(EventBusConfig eventBusConfig)
    {
        if (eventBusConfig.Connection != null)
        {
            string connJson = JsonConvert.SerializeObject(EventBusConfig?.Connection, new JsonSerializerSettings()
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
            });

            _connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connJson);
        }
        else
        {
            _connectionFactory = new ConnectionFactory();
        }
    }

    private void StartBasicConsume(string eventName)
    {
        if (_consumerChannel != null)
        {
            var consumer = new EventingBasicConsumer(_consumerChannel);
            consumer.Received += Consumer_Received;
            _consumerChannel.BasicConsume(queue: GetSubName(eventName),autoAck:false,consumer:consumer);
        }
    }

    private async void Consumer_Received(object? sender, BasicDeliverEventArgs e)
    {
        string eventName = e.RoutingKey;
        eventName = ProcessEventName(eventName);
        
        string message = Encoding.UTF8.GetString(e.Body.Span);

        try
        {
            await ProcessEvent(eventName, message);
        }
        catch (Exception exception)
        {
            // logging
        }
        
        _consumerChannel.BasicAck(e.DeliveryTag, false);
    }
}