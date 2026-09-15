import { execFileSync } from "node:child_process";
import { createHash, X509Certificate } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export interface WebTransportLoadnetCertificate {
  certificatePath: string;
  privateKeyPath: string;
  sha256: string;
}

export function ensureWebTransportLoadnetCertificate(
  dataDir: string,
): WebTransportLoadnetCertificate {
  const certificatePath = path.join(dataDir, "webtransport-cert.pem");
  const privateKeyPath = path.join(dataDir, "webtransport-key.pem");
  fs.mkdirSync(dataDir, { recursive: true });

  if (!fs.existsSync(certificatePath) || !fs.existsSync(privateKeyPath)) {
    execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-nodes",
        "-days",
        "13",
        "-subj",
        "/CN=cgp-loadnet",
        "-keyout",
        privateKeyPath,
        "-out",
        certificatePath,
        "-config",
        process.platform === "win32" ? "NUL" : "/dev/null",
      ],
      { stdio: "ignore" },
    );
  }

  const certificate = fs.readFileSync(certificatePath, "utf8");
  return {
    certificatePath,
    privateKeyPath,
    sha256: createHash("sha256")
      .update(new X509Certificate(certificate).raw)
      .digest("hex"),
  };
}
