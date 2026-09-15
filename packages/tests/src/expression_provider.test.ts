import { describe, expect, it } from "vitest";
import { createExpressionSearchProviderPlugin } from "@cgp/relay/src/plugins";

describe("external expression provider plugin", () => {
  it("advertises an external provider without implementing relay HTTP routes", () => {
    const plugin = createExpressionSearchProviderPlugin({
      id: "degif",
      label: "DeGIF",
      endpoint: "https://degif.example/plugins/hollow.expression.search/search",
      supportedTypes: ["gif"],
      acceptsMimeTypes: ["video/webm", "video/mp4", "image/webp"],
      tags: ["gif", "anime"],
    });

    expect(plugin.name).toBe("cgp.expression.search");
    expect(plugin.onHttp).toBeUndefined();
    expect(plugin.metadata?.expressionProvider).toMatchObject({
      id: "degif",
      label: "DeGIF",
      endpoint: "https://degif.example/plugins/hollow.expression.search/search",
      supportedTypes: ["gif"],
      acceptsMimeTypes: ["video/webm", "video/mp4", "image/webp"],
    });
  });

  it("rejects non-HTTP provider endpoints", () => {
    expect(() =>
      createExpressionSearchProviderPlugin({ endpoint: "file:///tmp/provider" }),
    ).toThrow(/HTTP or HTTPS/);
  });
});
