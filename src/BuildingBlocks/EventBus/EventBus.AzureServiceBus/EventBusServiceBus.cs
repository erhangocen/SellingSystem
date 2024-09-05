using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    
    private ITopicClient _topicClient;
    private ManagementClient _managementClient;
    private ILogger? _logger;
    
    public EventBusServiceBus(IServiceProvider serviceProvider, EventBusConfig eventBusConfig) : base(serviceProvider, eventBusConfig)
    {
        _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
        _managementClient = new ManagementClient(eventBusConfig.EventBusConnectionString);
        _topicClient = CreateTopicClient();
    }

    public override void Publish(IntegrationEvent @event)
    {
        string eventName = @event.GetType().Name;
        eventName = ProcessEventName(eventName);
        
        string eventStr = JsonConvert.SerializeObject(@event);
        byte[] eventBody = Encoding.UTF8.GetBytes(eventStr);

        Message message = new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Body = eventBody,
            Label = eventName
        };
        
        _topicClient.SendAsync(message).GetAwaiter().GetResult();
    }

    public override void Subscribe<T, TH>()
    {
        string eventName = typeof(T).Name;
        eventName = ProcessEventName(eventName);

        if (!SubscriptionManager.HasSubscriptionsForEvent(eventName))
        {
            ISubscriptionClient subscriptionClient = CreateSubscriptionClientIfNotExist(eventName);
            RegisterSubscriptionClientMessageHandler(subscriptionClient);
        }
        
        _logger.LogInformation($"Subscribing to event: {eventName} with {typeof(TH).Name}");
        
        SubscriptionManager.AddSubscription<T,TH>();
    }

    public override void UnSubscribe<T, TH>()
    {
        string eventName = typeof(T).Name;

        try
        {
            var subscriptionClient = CreateSubscriptionClient(eventName);
            subscriptionClient.RemoveRuleAsync(eventName).GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
            _logger?.LogWarning($"The messaging entity {eventName} was not found");
        }
        _logger?.LogInformation($"Unsubscribing from event: {eventName}");
        
        SubscriptionManager.RemoveSubscription<T,TH>();
    }

    public override void Dispose()
    {
        base.Dispose();
        
        _topicClient.CloseAsync().GetAwaiter().GetResult();
        _managementClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient = null;
        _managementClient = null;
        
    }

    private SubscriptionClient CreateSubscriptionClient(string eventName)
    {
        return new SubscriptionClient(EventBusConfig?.EventBusConnectionString, EventBusConfig?.DefaultTopicName,
            GetSubName(eventName));
    }

    private ISubscriptionClient CreateSubscriptionClientIfNotExist(string eventName)
    {
        SubscriptionClient subscriptionClient = CreateSubscriptionClient(eventName);
        bool subClientAlreadyExist = _managementClient
            .SubscriptionExistsAsync(EventBusConfig?.DefaultTopicName, eventName).GetAwaiter().GetResult();

        if (!subClientAlreadyExist)
        {
            _managementClient.CreateSubscriptionAsync(EventBusConfig?.DefaultTopicName, eventName).GetAwaiter().GetResult();
            RemoveDefaultRule(subscriptionClient);
        }
        CreateRuleIfNotExist(eventName, subscriptionClient);
        return subscriptionClient;
    }

    private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName).GetAwaiter().GetResult();
        }
        catch(MessagingEntityNotFoundException)
        {
            _logger?.LogWarning($"The messaging entity {EventBusConfig?.DefaultTopicName} was not found.");
        }
    }

    private void CreateRuleIfNotExist(string eventName, ISubscriptionClient subscriptionClient)
    {
        
        bool ruleExist;
        try
        {
            var rule = _managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName)
                .GetAwaiter().GetResult();

            ruleExist = rule != null;
        }
        catch (MessagingEntityNotFoundException)
        {
            ruleExist = false;
        }

        if (!ruleExist)
        {
            subscriptionClient.AddRuleAsync(new RuleDescription()
            {
                Name = eventName,
                Filter = new CorrelationFilter() { Label = eventName }
            }).GetAwaiter().GetResult();
        }
        
    }

    private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
    {
        Func<Message, CancellationToken, Task> messageHandler = async (message, token) =>
        {
            string eventName = $"{message.Label}";
            string messageData = Encoding.UTF8.GetString(message.Body);

            if (await ProcessEvent(ProcessEventName(eventName), messageData))
            {
                await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
            }
        };

        MessageHandlerOptions messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
        {
            MaxConcurrentCalls = 10,
            AutoComplete = false
        };
        
        subscriptionClient.RegisterMessageHandler(messageHandler, messageHandlerOptions);
    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        ExceptionReceivedContext context = exceptionReceivedEventArgs.ExceptionReceivedContext;
        Exception exception = exceptionReceivedEventArgs.Exception;
        
        _logger?.LogError(exception, $"Error Handling message : {exception.Message} - Context: {context}");

        return Task.CompletedTask;
    }

    private ITopicClient CreateTopicClient()
    {
        if (_topicClient == null || _topicClient.IsClosedOrClosing)
        {
            _topicClient = new TopicClient(EventBusConfig?.EventBusConnectionString, EventBusConfig?.DefaultTopicName, RetryPolicy.Default);
        }

        bool isExistTopic = _managementClient.TopicExistsAsync(EventBusConfig?.DefaultTopicName).GetAwaiter().GetResult();

        if (!isExistTopic)
        {
           _managementClient.CreateTopicAsync(EventBusConfig?.DefaultTopicName);
        }

        return _topicClient;
    }
}