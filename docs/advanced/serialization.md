# Serialization

The Durable Task Framework uses serialization to persist orchestration state, activity inputs/outputs, and messages between components. Understanding serialization is essential for correct orchestration behavior.

## Default Serialization

By default, DTFx uses JSON serialization via Newtonsoft.Json (Json.NET).

The default `JsonDataConverter` uses these settings:

```csharp
new JsonSerializerSettings
{
    TypeNameHandling = TypeNameHandling.Objects,
    DateParseHandling = DateParseHandling.None,
    SerializationBinder = new PackageUpgradeSerializationBinder()
}
```

**Key behaviors:**

- `TypeNameHandling.Objects` — Includes type information for polymorphic deserialization
- `DateParseHandling.None` — Dates are not automatically parsed (preserves as strings)
- `PackageUpgradeSerializationBinder` — Handles type name migration across package versions

## Custom DataConverter

### Creating a Custom Converter

Extend the abstract `DataConverter` class:

```csharp
using DurableTask.Core.Serializing;
using System.Text.Json;

public class SystemTextJsonDataConverter : DataConverter
{
    private readonly JsonSerializerOptions _options;
    
    public SystemTextJsonDataConverter()
    {
        _options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }
    
    public override string Serialize(object value)
    {
        return Serialize(value, formatted: false);
    }
    
    public override string Serialize(object value, bool formatted)
    {
        if (value == null)
        {
            return null;
        }
        
        var options = formatted
            ? new JsonSerializerOptions(_options) { WriteIndented = true }
            : _options;
            
        return JsonSerializer.Serialize(value, options);
    }
    
    public override object Deserialize(string data, Type objectType)
    {
        if (string.IsNullOrEmpty(data))
        {
            return null;
        }
        
        return JsonSerializer.Deserialize(data, objectType, _options);
    }
}
```

### Custom JsonSerializerSettings

For custom Newtonsoft.Json settings, pass settings to the constructor:

```csharp
var settings = new JsonSerializerSettings
{
    TypeNameHandling = TypeNameHandling.Auto,
    NullValueHandling = NullValueHandling.Ignore,
    DateFormatHandling = DateFormatHandling.IsoDateFormat,
    ContractResolver = new CamelCasePropertyNamesContractResolver()
};

var converter = new JsonDataConverter(settings);
```

### Using Custom Converters

Set custom converters on the `OrchestrationContext`:

```csharp
public class MyOrchestration : TaskOrchestration<string, Input>
{
    public override async Task<string> RunTask(
        OrchestrationContext context, 
        Input input)
    {
        // Use custom converter for messages (must be JsonDataConverter or subclass)
        context.MessageDataConverter = new JsonDataConverter(customSettings);
        
        // Use custom converter for errors (must be JsonDataConverter or subclass)
        context.ErrorDataConverter = new JsonDataConverter(customSettings);
        
        // Now all serialization uses custom converter
        var result = await context.ScheduleTask<Output>(
            typeof(MyActivity), 
            input);
        
        return result.ToString();
    }
}
```

> **Note:** The `MessageDataConverter` and `ErrorDataConverter` properties are typed as `JsonDataConverter`, not the base `DataConverter` class. To use completely custom serialization logic, you must subclass `JsonDataConverter` or use the `DataConverter` property on `TaskOrchestration` and `TaskActivity` classes instead.

## Activity Serialization

Activities also use `DataConverter`:

```csharp
public class MyActivity : TaskActivity<Input, Output>
{
    public MyActivity()
        : base(new CustomJsonDataConverter())  // Pass converter to base constructor
    {
    }
    
    protected override Output Execute(TaskContext context, Input input)
    {
        // input was deserialized with DataConverter
        // return value will be serialized with DataConverter
        return new Output { Value = input.Value * 2 };
    }
}
```

## Serialization Considerations

### Immutable Types

Use immutable types for orchestration inputs and outputs:

```csharp
// Good - immutable record
public record OrderInput(string OrderId, List<string> Items);

// Good - immutable class
public class OrderInput
{
    public OrderInput(string orderId, List<string> items)
    {
        OrderId = orderId;
        Items = items.ToList();  // Defensive copy
    }
    
    public string OrderId { get; }
    public IReadOnlyList<string> Items { get; }
}
```

### Polymorphic Types

When using inheritance, ensure proper type handling:

```csharp
// Base class
public abstract class PaymentMethod
{
    public string Id { get; set; }
}

// Derived classes
public class CreditCard : PaymentMethod
{
    public string CardNumber { get; set; }
}

public class BankTransfer : PaymentMethod
{
    public string AccountNumber { get; set; }
}

// TypeNameHandling.Objects (default) handles this correctly
var payment = new CreditCard { Id = "1", CardNumber = "4111..." };
var json = converter.Serialize(payment);
// json includes "$type" property for deserialization
```

### Circular References

Avoid circular references in serialized objects:

```csharp
// Bad - circular reference
public class Node
{
    public string Value { get; set; }
    public Node Parent { get; set; }  // Can create circular reference
    public List<Node> Children { get; set; }
}

// Better - use IDs for references
public class Node
{
    public string Id { get; set; }
    public string Value { get; set; }
    public string ParentId { get; set; }
    public List<string> ChildIds { get; set; }
}
```

If you must handle circular references:

```csharp
var settings = new JsonSerializerSettings
{
    TypeNameHandling = TypeNameHandling.Objects,
    ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
    PreserveReferencesHandling = PreserveReferencesHandling.Objects
};
```

### Large Payloads

Avoid large payloads in orchestration state:

```csharp
// Bad - large payload stored in state
public class BadInput
{
    public byte[] FileContent { get; set; }  // Could be megabytes
}

// Better - store reference, not content
public class BetterInput
{
    public string BlobUri { get; set; }  // Reference to blob storage
}

public override async Task<string> RunTask(
    OrchestrationContext context, 
    BetterInput input)
{
    // Activity downloads content when needed
    var content = await context.ScheduleTask<byte[]>(
        typeof(DownloadBlobActivity), 
        input.BlobUri);
    
    // Process and store result
    var resultUri = await context.ScheduleTask<string>(
        typeof(UploadResultActivity), 
        processedContent);
    
    return resultUri;
}
```

### Non-Serializable Types

Never include `CancellationToken` or other non-serializable runtime types in your input/output classes:

```csharp
// DANGEROUS - CancellationToken cannot be serialized safely
public class BadActivityInput
{
    public string Data { get; set; }
    public CancellationToken CancellationToken { get; set; }  // DO NOT DO THIS
}

// Good - pass cancellation token through method parameters, not serialized state
public class GoodActivityInput
{
    public string Data { get; set; }
}
```

> [!WARNING]
> Attempting to serialize `CancellationToken` can cause memory corruption, application crashes, and unpredictable behavior. The `CancellationToken` struct contains internal handles and references that are not designed for serialization.

Other types to avoid in serialized data:

- `CancellationToken` and `CancellationTokenSource`
- `Task` and `Task<T>`
- `Thread`, `Timer`, and other threading primitives
- `Stream` and its derivatives
- `HttpClient` and other network clients
- Any type holding unmanaged resources or handles

## Compression

For large payloads, consider compression:

```csharp
public class CompressedDataConverter : DataConverter
{
    private readonly JsonDataConverter _inner = JsonDataConverter.Default;
    
    public override string Serialize(object value)
    {
        string json = _inner.Serialize(value);
        return CompressString(json);
    }
    
    public override object Deserialize(string data, Type objectType)
    {
        string json = DecompressString(data);
        return _inner.Deserialize(json, objectType);
    }
    
    private string CompressString(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal))
        {
            gzip.Write(bytes, 0, bytes.Length);
        }
        return Convert.ToBase64String(output.ToArray());
    }
    
    private string DecompressString(string compressed)
    {
        var bytes = Convert.FromBase64String(compressed);
        using var input = new MemoryStream(bytes);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var reader = new StreamReader(gzip);
        return reader.ReadToEnd();
    }
}
```

## Version Compatibility

### Schema Evolution

Design for forward and backward compatibility:

```csharp
// Version 1
public class OrderV1
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
}

// Version 2 - added property
public class OrderV2
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "USD";  // Default for old data
}

// Version 3 - renamed property
public class OrderV3
{
    public string OrderId { get; set; }
    
    [JsonProperty("Amount")]  // Map old name
    public decimal TotalAmount { get; set; }
    
    public string Currency { get; set; } = "USD";
}
```

### Type Name Changes

When moving types between assemblies or namespaces:

```csharp
// Custom binder to handle type migrations
public class MySerializationBinder : ISerializationBinder
{
    public Type BindToType(string assemblyName, string typeName)
    {
        // Handle old type names
        if (typeName == "OldNamespace.MyType")
        {
            return typeof(NewNamespace.MyType);
        }
        
        // Fall back to default
        return Type.GetType($"{typeName}, {assemblyName}");
    }
    
    public void BindToName(Type serializedType, out string assemblyName, out string typeName)
    {
        assemblyName = serializedType.Assembly.FullName;
        typeName = serializedType.FullName;
    }
}
```

## Best Practices

### 1. Use Simple, Serializable Types

```csharp
// Good - simple POCO
public class OrderInput
{
    public string OrderId { get; set; }
    public List<string> ItemIds { get; set; }
    public decimal Total { get; set; }
}

// Avoid - complex types with behavior
public class OrderInput
{
    private readonly IOrderValidator _validator;  // Not serializable
    
    public void Validate() { /* ... */ }  // Behavior belongs elsewhere
}
```

### 2. Keep Payloads Small

```csharp
// Good - minimal data
public class ProcessingInput
{
    public string DocumentId { get; set; }
}

// Avoid - embedding large data
public class ProcessingInput
{
    public byte[] DocumentContent { get; set; }  // Could be huge
}
```

### 3. Be Explicit About Nullability

```csharp
public class OrderInput
{
    public string OrderId { get; set; }  // Required
    public string? CustomerNote { get; set; }  // Optional
    public List<string> Items { get; set; } = new();  // Never null
}
```

### 4. Test Serialization Round-Trips

```csharp
[Fact]
public void OrderInput_SerializesCorrectly()
{
    var input = new OrderInput
    {
        OrderId = "order-123",
        Items = new List<string> { "item-1", "item-2" }
    };
    
    var converter = JsonDataConverter.Default;
    string json = converter.Serialize(input);
    var deserialized = converter.Deserialize<OrderInput>(json);
    
    Assert.Equal(input.OrderId, deserialized.OrderId);
    Assert.Equal(input.Items, deserialized.Items);
}
```

## Next Steps

- [Testing](testing.md) — Testing orchestrations
- [Middleware](middleware.md) — Custom middleware
- [Entities](entities.md) — Durable Entities
