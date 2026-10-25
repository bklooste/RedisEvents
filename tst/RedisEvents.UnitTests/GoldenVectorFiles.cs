using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using FluentAssertions;

namespace RedisEvents.UnitTests;

/// <summary>
/// Reads and (when <c>REGENERATE_VECTORS=1</c>) writes the golden files under <c>tst/vectors/</c>.
/// </summary>
/// <remarks>
/// The files are shared with the Rust port, which tests itself against them. Every file is produced
/// by the real C# code in the golden-vector test classes; nothing here is hand-written expected output.
/// Output is pretty-printed JSON with LF line endings and a trailing newline, so a regenerated file
/// diffs cleanly.
/// </remarks>
internal static class GoldenVectorFiles
{
    private const string RegenerateVariable = "REGENERATE_VECTORS";

    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>True when the files should be rewritten instead of compared.</summary>
    internal static bool Regenerate => Environment.GetEnvironmentVariable(RegenerateVariable) == "1";

    /// <summary>The <c>tst/vectors</c> directory at the repository root.</summary>
    internal static string VectorsDirectory
    {
        get
        {
            var dir = new DirectoryInfo(AppContext.BaseDirectory);
            while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "RedisEvents.slnx")))
            {
                dir = dir.Parent;
            }

            dir.Should().NotBeNull("the test must run beneath the repository root (RedisEvents.slnx)");
            return Path.Combine(dir!.FullName, "tst", "vectors");
        }
    }

    /// <summary>
    /// Serialises <paramref name="value"/> and either writes it to <paramref name="fileName"/> (regenerate
    /// mode) or asserts the committed file matches it byte for byte.
    /// </summary>
    internal static void Check<T>(string fileName, T value)
    {
        var text = JsonSerializer.Serialize(value, Options) + "\n";
        var path = Path.Combine(VectorsDirectory, fileName);

        if (Regenerate)
        {
            Directory.CreateDirectory(VectorsDirectory);
            File.WriteAllText(path, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            return;
        }

        File.Exists(path).Should().BeTrue($"golden file {path} is missing; generate it with {RegenerateVariable}=1");
        var committed = File.ReadAllText(path, Encoding.UTF8).Replace("\r\n", "\n");
        committed.Should().Be(
            text,
            $"{fileName} no longer matches the C# code; if the change is intended, regenerate with {RegenerateVariable}=1 and review the diff");
    }
}
