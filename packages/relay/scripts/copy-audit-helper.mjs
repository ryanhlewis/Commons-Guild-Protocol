import {copyFile} from 'node:fs/promises';
// Keep the trusted data-only helper adjacent to emitted native_windows_storage_audit.js.
await copyFile(new URL('../src/windows_storage_audit.ps1',import.meta.url),new URL('../dist/windows_storage_audit.ps1',import.meta.url));
