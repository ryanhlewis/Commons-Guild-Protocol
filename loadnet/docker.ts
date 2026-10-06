/** Opt-in use of the native WSL engine; never changes the global Docker context. */
export function dockerBindPath(value: string, env: NodeJS.ProcessEnv = process.env): string {
    if (!env.CGP_LOADNET_DOCKER_WSL_DISTRO || !/^[A-Za-z]:[\\/]/.test(value)) return value.replaceAll("\\", "/");
    return `/mnt/${value[0].toLowerCase()}/${value.slice(3).replaceAll("\\", "/")}`;
}

export function dockerInvocation(args: string[], env: NodeJS.ProcessEnv = process.env) {
    const distro = env.CGP_LOADNET_DOCKER_WSL_DISTRO;
    if (!distro) return { command: "docker", args };
    if (!/^[a-zA-Z0-9_.-]+$/.test(distro)) throw new Error("Invalid WSL distribution name");
    return { command: "wsl.exe", args: ["-d", distro, "-u", "root", "--exec", "/usr/bin/docker", ...args.map(arg => /^[A-Za-z]:[\\/]/.test(arg) ? dockerBindPath(arg, env) : arg)] };
}
