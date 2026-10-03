import { test } from "node:test";
import assert from "node:assert/strict";
import { dockerBindPath, dockerInvocation } from "./docker";
test("native engine opt-in maps drive paths but preserves Docker arguments and named volumes", () => {
    const env = { CGP_LOADNET_DOCKER_WSL_DISTRO: "Ubuntu" };
    assert.equal(dockerBindPath("C:\\Users\\a b\\results", env), "/mnt/c/Users/a b/results");
    assert.equal(dockerBindPath("loadnet-data:/data", env), "loadnet-data:/data");
    assert.deepEqual(dockerInvocation(["compose", "-f", "D:/test/compose.yml"], env), {
        command: "wsl.exe", args: ["-d", "Ubuntu", "-u", "root", "--exec", "/usr/bin/docker", "compose", "-f", "/mnt/d/test/compose.yml"]
    });
    assert.throws(() => dockerInvocation([], {CGP_LOADNET_DOCKER_WSL_DISTRO:"Ubuntu; echo bad"}));
    assert.equal(dockerInvocation(["--format", "{\\\"ok\\\":true}"],env).args.at(-1), "{\\\"ok\\\":true}");
});
test("default Docker context is preserved without the opt-in", () => {
    assert.deepEqual(dockerInvocation(["version"], {}), {command:"docker",args:["version"]});
});
