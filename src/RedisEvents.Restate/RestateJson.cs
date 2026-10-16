using System.Text.Json.Serialization;

namespace RedisEvents.Restate;

/// <summary>
/// The body Restate's ingress answers a <c>/send</c> with, e.g.
/// <c>{"invocationId":"inv_1icq…","status":"Accepted"}</c> for a new workflow run and
/// <c>"status":"PreviouslyAccepted"</c> (same invocation id) when the workflow key was already
/// submitted, still within the service's <c>workflow_completion_retention</c>.
/// </summary>
/// <param name="InvocationId">The invocation id of the (first) run.</param>
/// <param name="Status"><c>Accepted</c> or <c>PreviouslyAccepted</c>.</param>
internal sealed record RestateSendResponse(
    [property: JsonPropertyName("invocationId")] string? InvocationId,
    [property: JsonPropertyName("status")] string? Status)
{
    /// <summary>The status Restate 1.8 gives a second submit of one workflow key.</summary>
    internal const string PreviouslyAccepted = "PreviouslyAccepted";
}

/// <summary>Source-generated metadata for the ingress response; nothing here uses reflection.</summary>
[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(RestateSendResponse))]
internal sealed partial class RestateJsonContext : JsonSerializerContext
{
}
