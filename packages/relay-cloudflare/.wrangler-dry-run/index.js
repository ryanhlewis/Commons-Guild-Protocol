var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/_internal/utils.mjs
// @__NO_SIDE_EFFECTS__
function createNotImplementedError(name) {
  return new Error(`[unenv] ${name} is not implemented yet!`);
}
// @__NO_SIDE_EFFECTS__
function notImplemented(name) {
  const fn = /* @__PURE__ */ __name(() => {
    throw /* @__PURE__ */ createNotImplementedError(name);
  }, "fn");
  return Object.assign(fn, { __unenv__: true });
}
var init_utils = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/_internal/utils.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    __name(createNotImplementedError, "createNotImplementedError");
    __name(notImplemented, "notImplemented");
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/perf_hooks/performance.mjs
var _timeOrigin, _performanceNow, nodeTiming, PerformanceEntry, PerformanceMark, PerformanceMeasure, PerformanceResourceTiming, PerformanceObserverEntryList, Performance, PerformanceObserver, performance;
var init_performance = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/perf_hooks/performance.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    init_utils();
    _timeOrigin = globalThis.performance?.timeOrigin ?? Date.now();
    _performanceNow = globalThis.performance?.now ? globalThis.performance.now.bind(globalThis.performance) : () => Date.now() - _timeOrigin;
    nodeTiming = {
      name: "node",
      entryType: "node",
      startTime: 0,
      duration: 0,
      nodeStart: 0,
      v8Start: 0,
      bootstrapComplete: 0,
      environment: 0,
      loopStart: 0,
      loopExit: 0,
      idleTime: 0,
      uvMetricsInfo: {
        loopCount: 0,
        events: 0,
        eventsWaiting: 0
      },
      detail: void 0,
      toJSON() {
        return this;
      }
    };
    PerformanceEntry = class {
      static {
        __name(this, "PerformanceEntry");
      }
      __unenv__ = true;
      detail;
      entryType = "event";
      name;
      startTime;
      constructor(name, options) {
        this.name = name;
        this.startTime = options?.startTime || _performanceNow();
        this.detail = options?.detail;
      }
      get duration() {
        return _performanceNow() - this.startTime;
      }
      toJSON() {
        return {
          name: this.name,
          entryType: this.entryType,
          startTime: this.startTime,
          duration: this.duration,
          detail: this.detail
        };
      }
    };
    PerformanceMark = class PerformanceMark2 extends PerformanceEntry {
      static {
        __name(this, "PerformanceMark");
      }
      entryType = "mark";
      constructor() {
        super(...arguments);
      }
      get duration() {
        return 0;
      }
    };
    PerformanceMeasure = class extends PerformanceEntry {
      static {
        __name(this, "PerformanceMeasure");
      }
      entryType = "measure";
    };
    PerformanceResourceTiming = class extends PerformanceEntry {
      static {
        __name(this, "PerformanceResourceTiming");
      }
      entryType = "resource";
      serverTiming = [];
      connectEnd = 0;
      connectStart = 0;
      decodedBodySize = 0;
      domainLookupEnd = 0;
      domainLookupStart = 0;
      encodedBodySize = 0;
      fetchStart = 0;
      initiatorType = "";
      name = "";
      nextHopProtocol = "";
      redirectEnd = 0;
      redirectStart = 0;
      requestStart = 0;
      responseEnd = 0;
      responseStart = 0;
      secureConnectionStart = 0;
      startTime = 0;
      transferSize = 0;
      workerStart = 0;
      responseStatus = 0;
    };
    PerformanceObserverEntryList = class {
      static {
        __name(this, "PerformanceObserverEntryList");
      }
      __unenv__ = true;
      getEntries() {
        return [];
      }
      getEntriesByName(_name, _type) {
        return [];
      }
      getEntriesByType(type) {
        return [];
      }
    };
    Performance = class {
      static {
        __name(this, "Performance");
      }
      __unenv__ = true;
      timeOrigin = _timeOrigin;
      eventCounts = /* @__PURE__ */ new Map();
      _entries = [];
      _resourceTimingBufferSize = 0;
      navigation = void 0;
      timing = void 0;
      timerify(_fn, _options) {
        throw createNotImplementedError("Performance.timerify");
      }
      get nodeTiming() {
        return nodeTiming;
      }
      eventLoopUtilization() {
        return {};
      }
      markResourceTiming() {
        return new PerformanceResourceTiming("");
      }
      onresourcetimingbufferfull = null;
      now() {
        if (this.timeOrigin === _timeOrigin) {
          return _performanceNow();
        }
        return Date.now() - this.timeOrigin;
      }
      clearMarks(markName) {
        this._entries = markName ? this._entries.filter((e) => e.name !== markName) : this._entries.filter((e) => e.entryType !== "mark");
      }
      clearMeasures(measureName) {
        this._entries = measureName ? this._entries.filter((e) => e.name !== measureName) : this._entries.filter((e) => e.entryType !== "measure");
      }
      clearResourceTimings() {
        this._entries = this._entries.filter((e) => e.entryType !== "resource" || e.entryType !== "navigation");
      }
      getEntries() {
        return this._entries;
      }
      getEntriesByName(name, type) {
        return this._entries.filter((e) => e.name === name && (!type || e.entryType === type));
      }
      getEntriesByType(type) {
        return this._entries.filter((e) => e.entryType === type);
      }
      mark(name, options) {
        const entry = new PerformanceMark(name, options);
        this._entries.push(entry);
        return entry;
      }
      measure(measureName, startOrMeasureOptions, endMark) {
        let start;
        let end;
        if (typeof startOrMeasureOptions === "string") {
          start = this.getEntriesByName(startOrMeasureOptions, "mark")[0]?.startTime;
          end = this.getEntriesByName(endMark, "mark")[0]?.startTime;
        } else {
          start = Number.parseFloat(startOrMeasureOptions?.start) || this.now();
          end = Number.parseFloat(startOrMeasureOptions?.end) || this.now();
        }
        const entry = new PerformanceMeasure(measureName, {
          startTime: start,
          detail: {
            start,
            end
          }
        });
        this._entries.push(entry);
        return entry;
      }
      setResourceTimingBufferSize(maxSize) {
        this._resourceTimingBufferSize = maxSize;
      }
      addEventListener(type, listener, options) {
        throw createNotImplementedError("Performance.addEventListener");
      }
      removeEventListener(type, listener, options) {
        throw createNotImplementedError("Performance.removeEventListener");
      }
      dispatchEvent(event) {
        throw createNotImplementedError("Performance.dispatchEvent");
      }
      toJSON() {
        return this;
      }
    };
    PerformanceObserver = class {
      static {
        __name(this, "PerformanceObserver");
      }
      __unenv__ = true;
      static supportedEntryTypes = [];
      _callback = null;
      constructor(callback) {
        this._callback = callback;
      }
      takeRecords() {
        return [];
      }
      disconnect() {
        throw createNotImplementedError("PerformanceObserver.disconnect");
      }
      observe(options) {
        throw createNotImplementedError("PerformanceObserver.observe");
      }
      bind(fn) {
        return fn;
      }
      runInAsyncScope(fn, thisArg, ...args) {
        return fn.call(thisArg, ...args);
      }
      asyncId() {
        return 0;
      }
      triggerAsyncId() {
        return 0;
      }
      emitDestroy() {
        return this;
      }
    };
    performance = globalThis.performance && "addEventListener" in globalThis.performance ? globalThis.performance : new Performance();
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/perf_hooks.mjs
var init_perf_hooks = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/perf_hooks.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    init_performance();
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/polyfill/performance.mjs
var init_performance2 = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/polyfill/performance.mjs"() {
    init_perf_hooks();
    globalThis.performance = performance;
    globalThis.Performance = Performance;
    globalThis.PerformanceEntry = PerformanceEntry;
    globalThis.PerformanceMark = PerformanceMark;
    globalThis.PerformanceMeasure = PerformanceMeasure;
    globalThis.PerformanceObserver = PerformanceObserver;
    globalThis.PerformanceObserverEntryList = PerformanceObserverEntryList;
    globalThis.PerformanceResourceTiming = PerformanceResourceTiming;
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/hrtime.mjs
var hrtime;
var init_hrtime = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/hrtime.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    hrtime = /* @__PURE__ */ Object.assign(/* @__PURE__ */ __name(function hrtime2(startTime) {
      const now = Date.now();
      const seconds = Math.trunc(now / 1e3);
      const nanos = now % 1e3 * 1e6;
      if (startTime) {
        let diffSeconds = seconds - startTime[0];
        let diffNanos = nanos - startTime[0];
        if (diffNanos < 0) {
          diffSeconds = diffSeconds - 1;
          diffNanos = 1e9 + diffNanos;
        }
        return [diffSeconds, diffNanos];
      }
      return [seconds, nanos];
    }, "hrtime"), { bigint: /* @__PURE__ */ __name(function bigint() {
      return BigInt(Date.now() * 1e6);
    }, "bigint") });
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/read-stream.mjs
var ReadStream;
var init_read_stream = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/read-stream.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    ReadStream = class {
      static {
        __name(this, "ReadStream");
      }
      fd;
      isRaw = false;
      isTTY = false;
      constructor(fd) {
        this.fd = fd;
      }
      setRawMode(mode) {
        this.isRaw = mode;
        return this;
      }
    };
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/write-stream.mjs
var WriteStream;
var init_write_stream = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/tty/write-stream.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    WriteStream = class {
      static {
        __name(this, "WriteStream");
      }
      fd;
      columns = 80;
      rows = 24;
      isTTY = false;
      constructor(fd) {
        this.fd = fd;
      }
      clearLine(dir, callback) {
        callback && callback();
        return false;
      }
      clearScreenDown(callback) {
        callback && callback();
        return false;
      }
      cursorTo(x, y, callback) {
        callback && typeof callback === "function" && callback();
        return false;
      }
      moveCursor(dx, dy, callback) {
        callback && callback();
        return false;
      }
      getColorDepth(env2) {
        return 1;
      }
      hasColors(count, env2) {
        return false;
      }
      getWindowSize() {
        return [this.columns, this.rows];
      }
      write(str, encoding, cb) {
        if (str instanceof Uint8Array) {
          str = new TextDecoder().decode(str);
        }
        try {
          console.log(str);
        } catch {
        }
        cb && typeof cb === "function" && cb();
        return false;
      }
    };
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/tty.mjs
var init_tty = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/tty.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    init_read_stream();
    init_write_stream();
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/node-version.mjs
var NODE_VERSION;
var init_node_version = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/node-version.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    NODE_VERSION = "22.14.0";
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/process.mjs
import { EventEmitter } from "node:events";
var Process;
var init_process = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/unenv/dist/runtime/node/internal/process/process.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    init_tty();
    init_utils();
    init_node_version();
    Process = class _Process extends EventEmitter {
      static {
        __name(this, "Process");
      }
      env;
      hrtime;
      nextTick;
      constructor(impl) {
        super();
        this.env = impl.env;
        this.hrtime = impl.hrtime;
        this.nextTick = impl.nextTick;
        for (const prop of [...Object.getOwnPropertyNames(_Process.prototype), ...Object.getOwnPropertyNames(EventEmitter.prototype)]) {
          const value = this[prop];
          if (typeof value === "function") {
            this[prop] = value.bind(this);
          }
        }
      }
      // --- event emitter ---
      emitWarning(warning, type, code) {
        console.warn(`${code ? `[${code}] ` : ""}${type ? `${type}: ` : ""}${warning}`);
      }
      emit(...args) {
        return super.emit(...args);
      }
      listeners(eventName) {
        return super.listeners(eventName);
      }
      // --- stdio (lazy initializers) ---
      #stdin;
      #stdout;
      #stderr;
      get stdin() {
        return this.#stdin ??= new ReadStream(0);
      }
      get stdout() {
        return this.#stdout ??= new WriteStream(1);
      }
      get stderr() {
        return this.#stderr ??= new WriteStream(2);
      }
      // --- cwd ---
      #cwd = "/";
      chdir(cwd2) {
        this.#cwd = cwd2;
      }
      cwd() {
        return this.#cwd;
      }
      // --- dummy props and getters ---
      arch = "";
      platform = "";
      argv = [];
      argv0 = "";
      execArgv = [];
      execPath = "";
      title = "";
      pid = 200;
      ppid = 100;
      get version() {
        return `v${NODE_VERSION}`;
      }
      get versions() {
        return { node: NODE_VERSION };
      }
      get allowedNodeEnvironmentFlags() {
        return /* @__PURE__ */ new Set();
      }
      get sourceMapsEnabled() {
        return false;
      }
      get debugPort() {
        return 0;
      }
      get throwDeprecation() {
        return false;
      }
      get traceDeprecation() {
        return false;
      }
      get features() {
        return {};
      }
      get release() {
        return {};
      }
      get connected() {
        return false;
      }
      get config() {
        return {};
      }
      get moduleLoadList() {
        return [];
      }
      constrainedMemory() {
        return 0;
      }
      availableMemory() {
        return 0;
      }
      uptime() {
        return 0;
      }
      resourceUsage() {
        return {};
      }
      // --- noop methods ---
      ref() {
      }
      unref() {
      }
      // --- unimplemented methods ---
      umask() {
        throw createNotImplementedError("process.umask");
      }
      getBuiltinModule() {
        return void 0;
      }
      getActiveResourcesInfo() {
        throw createNotImplementedError("process.getActiveResourcesInfo");
      }
      exit() {
        throw createNotImplementedError("process.exit");
      }
      reallyExit() {
        throw createNotImplementedError("process.reallyExit");
      }
      kill() {
        throw createNotImplementedError("process.kill");
      }
      abort() {
        throw createNotImplementedError("process.abort");
      }
      dlopen() {
        throw createNotImplementedError("process.dlopen");
      }
      setSourceMapsEnabled() {
        throw createNotImplementedError("process.setSourceMapsEnabled");
      }
      loadEnvFile() {
        throw createNotImplementedError("process.loadEnvFile");
      }
      disconnect() {
        throw createNotImplementedError("process.disconnect");
      }
      cpuUsage() {
        throw createNotImplementedError("process.cpuUsage");
      }
      setUncaughtExceptionCaptureCallback() {
        throw createNotImplementedError("process.setUncaughtExceptionCaptureCallback");
      }
      hasUncaughtExceptionCaptureCallback() {
        throw createNotImplementedError("process.hasUncaughtExceptionCaptureCallback");
      }
      initgroups() {
        throw createNotImplementedError("process.initgroups");
      }
      openStdin() {
        throw createNotImplementedError("process.openStdin");
      }
      assert() {
        throw createNotImplementedError("process.assert");
      }
      binding() {
        throw createNotImplementedError("process.binding");
      }
      // --- attached interfaces ---
      permission = { has: /* @__PURE__ */ notImplemented("process.permission.has") };
      report = {
        directory: "",
        filename: "",
        signal: "SIGUSR2",
        compact: false,
        reportOnFatalError: false,
        reportOnSignal: false,
        reportOnUncaughtException: false,
        getReport: /* @__PURE__ */ notImplemented("process.report.getReport"),
        writeReport: /* @__PURE__ */ notImplemented("process.report.writeReport")
      };
      finalization = {
        register: /* @__PURE__ */ notImplemented("process.finalization.register"),
        unregister: /* @__PURE__ */ notImplemented("process.finalization.unregister"),
        registerBeforeExit: /* @__PURE__ */ notImplemented("process.finalization.registerBeforeExit")
      };
      memoryUsage = Object.assign(() => ({
        arrayBuffers: 0,
        rss: 0,
        external: 0,
        heapTotal: 0,
        heapUsed: 0
      }), { rss: /* @__PURE__ */ __name(() => 0, "rss") });
      // --- undefined props ---
      mainModule = void 0;
      domain = void 0;
      // optional
      send = void 0;
      exitCode = void 0;
      channel = void 0;
      getegid = void 0;
      geteuid = void 0;
      getgid = void 0;
      getgroups = void 0;
      getuid = void 0;
      setegid = void 0;
      seteuid = void 0;
      setgid = void 0;
      setgroups = void 0;
      setuid = void 0;
      // internals
      _events = void 0;
      _eventsCount = void 0;
      _exiting = void 0;
      _maxListeners = void 0;
      _debugEnd = void 0;
      _debugProcess = void 0;
      _fatalException = void 0;
      _getActiveHandles = void 0;
      _getActiveRequests = void 0;
      _kill = void 0;
      _preload_modules = void 0;
      _rawDebug = void 0;
      _startProfilerIdleNotifier = void 0;
      _stopProfilerIdleNotifier = void 0;
      _tickCallback = void 0;
      _disconnect = void 0;
      _handleQueue = void 0;
      _pendingMessage = void 0;
      _channel = void 0;
      _send = void 0;
      _linkedBinding = void 0;
    };
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/node/process.mjs
var globalProcess, getBuiltinModule, workerdProcess, isWorkerdProcessV2, unenvProcess, exit, features, platform, env, hrtime3, nextTick, _channel, _disconnect, _events, _eventsCount, _handleQueue, _maxListeners, _pendingMessage, _send, assert, disconnect, mainModule, _debugEnd, _debugProcess, _exiting, _fatalException, _getActiveHandles, _getActiveRequests, _kill, _linkedBinding, _preload_modules, _rawDebug, _startProfilerIdleNotifier, _stopProfilerIdleNotifier, _tickCallback, abort, addListener, allowedNodeEnvironmentFlags, arch, argv, argv0, availableMemory, binding, channel, chdir, config, connected, constrainedMemory, cpuUsage, cwd, debugPort, dlopen, domain, emit, emitWarning, eventNames, execArgv, execPath, exitCode, finalization, getActiveResourcesInfo, getegid, geteuid, getgid, getgroups, getMaxListeners, getuid, hasUncaughtExceptionCaptureCallback, initgroups, kill, listenerCount, listeners, loadEnvFile, memoryUsage, moduleLoadList, off, on, once, openStdin, permission, pid, ppid, prependListener, prependOnceListener, rawListeners, reallyExit, ref, release, removeAllListeners, removeListener, report, resourceUsage, send, setegid, seteuid, setgid, setgroups, setMaxListeners, setSourceMapsEnabled, setuid, setUncaughtExceptionCaptureCallback, sourceMapsEnabled, stderr, stdin, stdout, throwDeprecation, title, traceDeprecation, umask, unref, uptime, version, versions, _process, process_default;
var init_process2 = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/node_modules/@cloudflare/unenv-preset/dist/runtime/node/process.mjs"() {
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    init_hrtime();
    init_process();
    globalProcess = globalThis["process"];
    getBuiltinModule = globalProcess.getBuiltinModule;
    workerdProcess = getBuiltinModule("node:process");
    isWorkerdProcessV2 = globalThis.Cloudflare.compatibilityFlags.enable_nodejs_process_v2;
    unenvProcess = new Process({
      env: globalProcess.env,
      // `hrtime` is only available from workerd process v2
      hrtime: isWorkerdProcessV2 ? workerdProcess.hrtime : hrtime,
      // `nextTick` is available from workerd process v1
      nextTick: workerdProcess.nextTick
    });
    ({ exit, features, platform } = workerdProcess);
    ({
      env: (
        // Always implemented by workerd
        env
      ),
      hrtime: (
        // Only implemented in workerd v2
        hrtime3
      ),
      nextTick: (
        // Always implemented by workerd
        nextTick
      )
    } = unenvProcess);
    ({
      _channel,
      _disconnect,
      _events,
      _eventsCount,
      _handleQueue,
      _maxListeners,
      _pendingMessage,
      _send,
      assert,
      disconnect,
      mainModule
    } = unenvProcess);
    ({
      _debugEnd: (
        // @ts-expect-error `_debugEnd` is missing typings
        _debugEnd
      ),
      _debugProcess: (
        // @ts-expect-error `_debugProcess` is missing typings
        _debugProcess
      ),
      _exiting: (
        // @ts-expect-error `_exiting` is missing typings
        _exiting
      ),
      _fatalException: (
        // @ts-expect-error `_fatalException` is missing typings
        _fatalException
      ),
      _getActiveHandles: (
        // @ts-expect-error `_getActiveHandles` is missing typings
        _getActiveHandles
      ),
      _getActiveRequests: (
        // @ts-expect-error `_getActiveRequests` is missing typings
        _getActiveRequests
      ),
      _kill: (
        // @ts-expect-error `_kill` is missing typings
        _kill
      ),
      _linkedBinding: (
        // @ts-expect-error `_linkedBinding` is missing typings
        _linkedBinding
      ),
      _preload_modules: (
        // @ts-expect-error `_preload_modules` is missing typings
        _preload_modules
      ),
      _rawDebug: (
        // @ts-expect-error `_rawDebug` is missing typings
        _rawDebug
      ),
      _startProfilerIdleNotifier: (
        // @ts-expect-error `_startProfilerIdleNotifier` is missing typings
        _startProfilerIdleNotifier
      ),
      _stopProfilerIdleNotifier: (
        // @ts-expect-error `_stopProfilerIdleNotifier` is missing typings
        _stopProfilerIdleNotifier
      ),
      _tickCallback: (
        // @ts-expect-error `_tickCallback` is missing typings
        _tickCallback
      ),
      abort,
      addListener,
      allowedNodeEnvironmentFlags,
      arch,
      argv,
      argv0,
      availableMemory,
      binding: (
        // @ts-expect-error `binding` is missing typings
        binding
      ),
      channel,
      chdir,
      config,
      connected,
      constrainedMemory,
      cpuUsage,
      cwd,
      debugPort,
      dlopen,
      domain: (
        // @ts-expect-error `domain` is missing typings
        domain
      ),
      emit,
      emitWarning,
      eventNames,
      execArgv,
      execPath,
      exitCode,
      finalization,
      getActiveResourcesInfo,
      getegid,
      geteuid,
      getgid,
      getgroups,
      getMaxListeners,
      getuid,
      hasUncaughtExceptionCaptureCallback,
      initgroups: (
        // @ts-expect-error `initgroups` is missing typings
        initgroups
      ),
      kill,
      listenerCount,
      listeners,
      loadEnvFile,
      memoryUsage,
      moduleLoadList: (
        // @ts-expect-error `moduleLoadList` is missing typings
        moduleLoadList
      ),
      off,
      on,
      once,
      openStdin: (
        // @ts-expect-error `openStdin` is missing typings
        openStdin
      ),
      permission,
      pid,
      ppid,
      prependListener,
      prependOnceListener,
      rawListeners,
      reallyExit: (
        // @ts-expect-error `reallyExit` is missing typings
        reallyExit
      ),
      ref,
      release,
      removeAllListeners,
      removeListener,
      report,
      resourceUsage,
      send,
      setegid,
      seteuid,
      setgid,
      setgroups,
      setMaxListeners,
      setSourceMapsEnabled,
      setuid,
      setUncaughtExceptionCaptureCallback,
      sourceMapsEnabled,
      stderr,
      stdin,
      stdout,
      throwDeprecation,
      title,
      traceDeprecation,
      umask,
      unref,
      uptime,
      version,
      versions
    } = isWorkerdProcessV2 ? workerdProcess : unenvProcess);
    _process = {
      abort,
      addListener,
      allowedNodeEnvironmentFlags,
      hasUncaughtExceptionCaptureCallback,
      setUncaughtExceptionCaptureCallback,
      loadEnvFile,
      sourceMapsEnabled,
      arch,
      argv,
      argv0,
      chdir,
      config,
      connected,
      constrainedMemory,
      availableMemory,
      cpuUsage,
      cwd,
      debugPort,
      dlopen,
      disconnect,
      emit,
      emitWarning,
      env,
      eventNames,
      execArgv,
      execPath,
      exit,
      finalization,
      features,
      getBuiltinModule,
      getActiveResourcesInfo,
      getMaxListeners,
      hrtime: hrtime3,
      kill,
      listeners,
      listenerCount,
      memoryUsage,
      nextTick,
      on,
      off,
      once,
      pid,
      platform,
      ppid,
      prependListener,
      prependOnceListener,
      rawListeners,
      release,
      removeAllListeners,
      removeListener,
      report,
      resourceUsage,
      setMaxListeners,
      setSourceMapsEnabled,
      stderr,
      stdin,
      stdout,
      title,
      throwDeprecation,
      traceDeprecation,
      umask,
      uptime,
      version,
      versions,
      // @ts-expect-error old API
      domain,
      initgroups,
      moduleLoadList,
      reallyExit,
      openStdin,
      assert,
      binding,
      send,
      exitCode,
      channel,
      getegid,
      geteuid,
      getgid,
      getgroups,
      getuid,
      setegid,
      seteuid,
      setgid,
      setgroups,
      setuid,
      permission,
      mainModule,
      _events,
      _eventsCount,
      _exiting,
      _maxListeners,
      _debugEnd,
      _debugProcess,
      _fatalException,
      _getActiveHandles,
      _getActiveRequests,
      _kill,
      _preload_modules,
      _rawDebug,
      _startProfilerIdleNotifier,
      _stopProfilerIdleNotifier,
      _tickCallback,
      _disconnect,
      _handleQueue,
      _pendingMessage,
      _channel,
      _send,
      _linkedBinding
    };
    process_default = _process;
  }
});

// ../../../../../AppData/Roaming/npm/node_modules/wrangler/_virtual_unenv_global_polyfill-@cloudflare-unenv-preset-node-process
var init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process = __esm({
  "../../../../../AppData/Roaming/npm/node_modules/wrangler/_virtual_unenv_global_polyfill-@cloudflare-unenv-preset-node-process"() {
    init_process2();
    globalThis.process = process_default;
  }
});

// ../../node_modules/safe-stable-stringify/index.js
var require_safe_stable_stringify = __commonJS({
  "../../node_modules/safe-stable-stringify/index.js"(exports, module) {
    "use strict";
    init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
    init_performance2();
    var { hasOwnProperty } = Object.prototype;
    var stringify = configure2();
    stringify.configure = configure2;
    stringify.stringify = stringify;
    stringify.default = stringify;
    exports.stringify = stringify;
    exports.configure = configure2;
    module.exports = stringify;
    var strEscapeSequencesRegExp = /[\u0000-\u001f\u0022\u005c\ud800-\udfff]/;
    function strEscape(str) {
      if (str.length < 5e3 && !strEscapeSequencesRegExp.test(str)) {
        return `"${str}"`;
      }
      return JSON.stringify(str);
    }
    __name(strEscape, "strEscape");
    function sort(array, comparator) {
      if (array.length > 200 || comparator) {
        return array.sort(comparator);
      }
      for (let i = 1; i < array.length; i++) {
        const currentValue = array[i];
        let position = i;
        while (position !== 0 && array[position - 1] > currentValue) {
          array[position] = array[position - 1];
          position--;
        }
        array[position] = currentValue;
      }
      return array;
    }
    __name(sort, "sort");
    var typedArrayPrototypeGetSymbolToStringTag = Object.getOwnPropertyDescriptor(
      Object.getPrototypeOf(
        Object.getPrototypeOf(
          new Int8Array()
        )
      ),
      Symbol.toStringTag
    ).get;
    function isTypedArrayWithEntries(value) {
      return typedArrayPrototypeGetSymbolToStringTag.call(value) !== void 0 && value.length !== 0;
    }
    __name(isTypedArrayWithEntries, "isTypedArrayWithEntries");
    function stringifyTypedArray(array, separator, maximumBreadth) {
      if (array.length < maximumBreadth) {
        maximumBreadth = array.length;
      }
      const whitespace = separator === "," ? "" : " ";
      let res = `"0":${whitespace}${array[0]}`;
      for (let i = 1; i < maximumBreadth; i++) {
        res += `${separator}"${i}":${whitespace}${array[i]}`;
      }
      return res;
    }
    __name(stringifyTypedArray, "stringifyTypedArray");
    function getCircularValueOption(options) {
      if (hasOwnProperty.call(options, "circularValue")) {
        const circularValue = options.circularValue;
        if (typeof circularValue === "string") {
          return `"${circularValue}"`;
        }
        if (circularValue == null) {
          return circularValue;
        }
        if (circularValue === Error || circularValue === TypeError) {
          return {
            toString() {
              throw new TypeError("Converting circular structure to JSON");
            }
          };
        }
        throw new TypeError('The "circularValue" argument must be of type string or the value null or undefined');
      }
      return '"[Circular]"';
    }
    __name(getCircularValueOption, "getCircularValueOption");
    function getDeterministicOption(options) {
      let value;
      if (hasOwnProperty.call(options, "deterministic")) {
        value = options.deterministic;
        if (typeof value !== "boolean" && typeof value !== "function") {
          throw new TypeError('The "deterministic" argument must be of type boolean or comparator function');
        }
      }
      return value === void 0 ? true : value;
    }
    __name(getDeterministicOption, "getDeterministicOption");
    function getBooleanOption(options, key) {
      let value;
      if (hasOwnProperty.call(options, key)) {
        value = options[key];
        if (typeof value !== "boolean") {
          throw new TypeError(`The "${key}" argument must be of type boolean`);
        }
      }
      return value === void 0 ? true : value;
    }
    __name(getBooleanOption, "getBooleanOption");
    function getPositiveIntegerOption(options, key) {
      let value;
      if (hasOwnProperty.call(options, key)) {
        value = options[key];
        if (typeof value !== "number") {
          throw new TypeError(`The "${key}" argument must be of type number`);
        }
        if (!Number.isInteger(value)) {
          throw new TypeError(`The "${key}" argument must be an integer`);
        }
        if (value < 1) {
          throw new RangeError(`The "${key}" argument must be >= 1`);
        }
      }
      return value === void 0 ? Infinity : value;
    }
    __name(getPositiveIntegerOption, "getPositiveIntegerOption");
    function getItemCount(number) {
      if (number === 1) {
        return "1 item";
      }
      return `${number} items`;
    }
    __name(getItemCount, "getItemCount");
    function getUniqueReplacerSet(replacerArray) {
      const replacerSet = /* @__PURE__ */ new Set();
      for (const value of replacerArray) {
        if (typeof value === "string" || typeof value === "number") {
          replacerSet.add(String(value));
        }
      }
      return replacerSet;
    }
    __name(getUniqueReplacerSet, "getUniqueReplacerSet");
    function getStrictOption(options) {
      if (hasOwnProperty.call(options, "strict")) {
        const value = options.strict;
        if (typeof value !== "boolean") {
          throw new TypeError('The "strict" argument must be of type boolean');
        }
        if (value) {
          return (value2) => {
            let message = `Object can not safely be stringified. Received type ${typeof value2}`;
            if (typeof value2 !== "function") message += ` (${value2.toString()})`;
            throw new Error(message);
          };
        }
      }
    }
    __name(getStrictOption, "getStrictOption");
    function configure2(options) {
      options = { ...options };
      const fail = getStrictOption(options);
      if (fail) {
        if (options.bigint === void 0) {
          options.bigint = false;
        }
        if (!("circularValue" in options)) {
          options.circularValue = Error;
        }
      }
      const circularValue = getCircularValueOption(options);
      const bigint2 = getBooleanOption(options, "bigint");
      const deterministic = getDeterministicOption(options);
      const comparator = typeof deterministic === "function" ? deterministic : void 0;
      const maximumDepth = getPositiveIntegerOption(options, "maximumDepth");
      const maximumBreadth = getPositiveIntegerOption(options, "maximumBreadth");
      function stringifyFnReplacer(key, parent, stack, replacer, spacer, indentation) {
        let value = parent[key];
        if (typeof value === "object" && value !== null && typeof value.toJSON === "function") {
          value = value.toJSON(key);
        }
        value = replacer.call(parent, key, value);
        switch (typeof value) {
          case "string":
            return strEscape(value);
          case "object": {
            if (value === null) {
              return "null";
            }
            if (stack.indexOf(value) !== -1) {
              return circularValue;
            }
            let res = "";
            let join = ",";
            const originalIndentation = indentation;
            if (Array.isArray(value)) {
              if (value.length === 0) {
                return "[]";
              }
              if (maximumDepth < stack.length + 1) {
                return '"[Array]"';
              }
              stack.push(value);
              if (spacer !== "") {
                indentation += spacer;
                res += `
${indentation}`;
                join = `,
${indentation}`;
              }
              const maximumValuesToStringify = Math.min(value.length, maximumBreadth);
              let i = 0;
              for (; i < maximumValuesToStringify - 1; i++) {
                const tmp2 = stringifyFnReplacer(String(i), value, stack, replacer, spacer, indentation);
                res += tmp2 !== void 0 ? tmp2 : "null";
                res += join;
              }
              const tmp = stringifyFnReplacer(String(i), value, stack, replacer, spacer, indentation);
              res += tmp !== void 0 ? tmp : "null";
              if (value.length - 1 > maximumBreadth) {
                const removedKeys = value.length - maximumBreadth - 1;
                res += `${join}"... ${getItemCount(removedKeys)} not stringified"`;
              }
              if (spacer !== "") {
                res += `
${originalIndentation}`;
              }
              stack.pop();
              return `[${res}]`;
            }
            let keys = Object.keys(value);
            const keyLength = keys.length;
            if (keyLength === 0) {
              return "{}";
            }
            if (maximumDepth < stack.length + 1) {
              return '"[Object]"';
            }
            let whitespace = "";
            let separator = "";
            if (spacer !== "") {
              indentation += spacer;
              join = `,
${indentation}`;
              whitespace = " ";
            }
            const maximumPropertiesToStringify = Math.min(keyLength, maximumBreadth);
            if (deterministic && !isTypedArrayWithEntries(value)) {
              keys = sort(keys, comparator);
            }
            stack.push(value);
            for (let i = 0; i < maximumPropertiesToStringify; i++) {
              const key2 = keys[i];
              const tmp = stringifyFnReplacer(key2, value, stack, replacer, spacer, indentation);
              if (tmp !== void 0) {
                res += `${separator}${strEscape(key2)}:${whitespace}${tmp}`;
                separator = join;
              }
            }
            if (keyLength > maximumBreadth) {
              const removedKeys = keyLength - maximumBreadth;
              res += `${separator}"...":${whitespace}"${getItemCount(removedKeys)} not stringified"`;
              separator = join;
            }
            if (spacer !== "" && separator.length > 1) {
              res = `
${indentation}${res}
${originalIndentation}`;
            }
            stack.pop();
            return `{${res}}`;
          }
          case "number":
            return isFinite(value) ? String(value) : fail ? fail(value) : "null";
          case "boolean":
            return value === true ? "true" : "false";
          case "undefined":
            return void 0;
          case "bigint":
            if (bigint2) {
              return String(value);
            }
          // fallthrough
          default:
            return fail ? fail(value) : void 0;
        }
      }
      __name(stringifyFnReplacer, "stringifyFnReplacer");
      function stringifyArrayReplacer(key, value, stack, replacer, spacer, indentation) {
        if (typeof value === "object" && value !== null && typeof value.toJSON === "function") {
          value = value.toJSON(key);
        }
        switch (typeof value) {
          case "string":
            return strEscape(value);
          case "object": {
            if (value === null) {
              return "null";
            }
            if (stack.indexOf(value) !== -1) {
              return circularValue;
            }
            const originalIndentation = indentation;
            let res = "";
            let join = ",";
            if (Array.isArray(value)) {
              if (value.length === 0) {
                return "[]";
              }
              if (maximumDepth < stack.length + 1) {
                return '"[Array]"';
              }
              stack.push(value);
              if (spacer !== "") {
                indentation += spacer;
                res += `
${indentation}`;
                join = `,
${indentation}`;
              }
              const maximumValuesToStringify = Math.min(value.length, maximumBreadth);
              let i = 0;
              for (; i < maximumValuesToStringify - 1; i++) {
                const tmp2 = stringifyArrayReplacer(String(i), value[i], stack, replacer, spacer, indentation);
                res += tmp2 !== void 0 ? tmp2 : "null";
                res += join;
              }
              const tmp = stringifyArrayReplacer(String(i), value[i], stack, replacer, spacer, indentation);
              res += tmp !== void 0 ? tmp : "null";
              if (value.length - 1 > maximumBreadth) {
                const removedKeys = value.length - maximumBreadth - 1;
                res += `${join}"... ${getItemCount(removedKeys)} not stringified"`;
              }
              if (spacer !== "") {
                res += `
${originalIndentation}`;
              }
              stack.pop();
              return `[${res}]`;
            }
            stack.push(value);
            let whitespace = "";
            if (spacer !== "") {
              indentation += spacer;
              join = `,
${indentation}`;
              whitespace = " ";
            }
            let separator = "";
            for (const key2 of replacer) {
              const tmp = stringifyArrayReplacer(key2, value[key2], stack, replacer, spacer, indentation);
              if (tmp !== void 0) {
                res += `${separator}${strEscape(key2)}:${whitespace}${tmp}`;
                separator = join;
              }
            }
            if (spacer !== "" && separator.length > 1) {
              res = `
${indentation}${res}
${originalIndentation}`;
            }
            stack.pop();
            return `{${res}}`;
          }
          case "number":
            return isFinite(value) ? String(value) : fail ? fail(value) : "null";
          case "boolean":
            return value === true ? "true" : "false";
          case "undefined":
            return void 0;
          case "bigint":
            if (bigint2) {
              return String(value);
            }
          // fallthrough
          default:
            return fail ? fail(value) : void 0;
        }
      }
      __name(stringifyArrayReplacer, "stringifyArrayReplacer");
      function stringifyIndent(key, value, stack, spacer, indentation) {
        switch (typeof value) {
          case "string":
            return strEscape(value);
          case "object": {
            if (value === null) {
              return "null";
            }
            if (typeof value.toJSON === "function") {
              value = value.toJSON(key);
              if (typeof value !== "object") {
                return stringifyIndent(key, value, stack, spacer, indentation);
              }
              if (value === null) {
                return "null";
              }
            }
            if (stack.indexOf(value) !== -1) {
              return circularValue;
            }
            const originalIndentation = indentation;
            if (Array.isArray(value)) {
              if (value.length === 0) {
                return "[]";
              }
              if (maximumDepth < stack.length + 1) {
                return '"[Array]"';
              }
              stack.push(value);
              indentation += spacer;
              let res2 = `
${indentation}`;
              const join2 = `,
${indentation}`;
              const maximumValuesToStringify = Math.min(value.length, maximumBreadth);
              let i = 0;
              for (; i < maximumValuesToStringify - 1; i++) {
                const tmp2 = stringifyIndent(String(i), value[i], stack, spacer, indentation);
                res2 += tmp2 !== void 0 ? tmp2 : "null";
                res2 += join2;
              }
              const tmp = stringifyIndent(String(i), value[i], stack, spacer, indentation);
              res2 += tmp !== void 0 ? tmp : "null";
              if (value.length - 1 > maximumBreadth) {
                const removedKeys = value.length - maximumBreadth - 1;
                res2 += `${join2}"... ${getItemCount(removedKeys)} not stringified"`;
              }
              res2 += `
${originalIndentation}`;
              stack.pop();
              return `[${res2}]`;
            }
            let keys = Object.keys(value);
            const keyLength = keys.length;
            if (keyLength === 0) {
              return "{}";
            }
            if (maximumDepth < stack.length + 1) {
              return '"[Object]"';
            }
            indentation += spacer;
            const join = `,
${indentation}`;
            let res = "";
            let separator = "";
            let maximumPropertiesToStringify = Math.min(keyLength, maximumBreadth);
            if (isTypedArrayWithEntries(value)) {
              res += stringifyTypedArray(value, join, maximumBreadth);
              keys = keys.slice(value.length);
              maximumPropertiesToStringify -= value.length;
              separator = join;
            }
            if (deterministic) {
              keys = sort(keys, comparator);
            }
            stack.push(value);
            for (let i = 0; i < maximumPropertiesToStringify; i++) {
              const key2 = keys[i];
              const tmp = stringifyIndent(key2, value[key2], stack, spacer, indentation);
              if (tmp !== void 0) {
                res += `${separator}${strEscape(key2)}: ${tmp}`;
                separator = join;
              }
            }
            if (keyLength > maximumBreadth) {
              const removedKeys = keyLength - maximumBreadth;
              res += `${separator}"...": "${getItemCount(removedKeys)} not stringified"`;
              separator = join;
            }
            if (separator !== "") {
              res = `
${indentation}${res}
${originalIndentation}`;
            }
            stack.pop();
            return `{${res}}`;
          }
          case "number":
            return isFinite(value) ? String(value) : fail ? fail(value) : "null";
          case "boolean":
            return value === true ? "true" : "false";
          case "undefined":
            return void 0;
          case "bigint":
            if (bigint2) {
              return String(value);
            }
          // fallthrough
          default:
            return fail ? fail(value) : void 0;
        }
      }
      __name(stringifyIndent, "stringifyIndent");
      function stringifySimple(key, value, stack) {
        switch (typeof value) {
          case "string":
            return strEscape(value);
          case "object": {
            if (value === null) {
              return "null";
            }
            if (typeof value.toJSON === "function") {
              value = value.toJSON(key);
              if (typeof value !== "object") {
                return stringifySimple(key, value, stack);
              }
              if (value === null) {
                return "null";
              }
            }
            if (stack.indexOf(value) !== -1) {
              return circularValue;
            }
            let res = "";
            const hasLength = value.length !== void 0;
            if (hasLength && Array.isArray(value)) {
              if (value.length === 0) {
                return "[]";
              }
              if (maximumDepth < stack.length + 1) {
                return '"[Array]"';
              }
              stack.push(value);
              const maximumValuesToStringify = Math.min(value.length, maximumBreadth);
              let i = 0;
              for (; i < maximumValuesToStringify - 1; i++) {
                const tmp2 = stringifySimple(String(i), value[i], stack);
                res += tmp2 !== void 0 ? tmp2 : "null";
                res += ",";
              }
              const tmp = stringifySimple(String(i), value[i], stack);
              res += tmp !== void 0 ? tmp : "null";
              if (value.length - 1 > maximumBreadth) {
                const removedKeys = value.length - maximumBreadth - 1;
                res += `,"... ${getItemCount(removedKeys)} not stringified"`;
              }
              stack.pop();
              return `[${res}]`;
            }
            let keys = Object.keys(value);
            const keyLength = keys.length;
            if (keyLength === 0) {
              return "{}";
            }
            if (maximumDepth < stack.length + 1) {
              return '"[Object]"';
            }
            let separator = "";
            let maximumPropertiesToStringify = Math.min(keyLength, maximumBreadth);
            if (hasLength && isTypedArrayWithEntries(value)) {
              res += stringifyTypedArray(value, ",", maximumBreadth);
              keys = keys.slice(value.length);
              maximumPropertiesToStringify -= value.length;
              separator = ",";
            }
            if (deterministic) {
              keys = sort(keys, comparator);
            }
            stack.push(value);
            for (let i = 0; i < maximumPropertiesToStringify; i++) {
              const key2 = keys[i];
              const tmp = stringifySimple(key2, value[key2], stack);
              if (tmp !== void 0) {
                res += `${separator}${strEscape(key2)}:${tmp}`;
                separator = ",";
              }
            }
            if (keyLength > maximumBreadth) {
              const removedKeys = keyLength - maximumBreadth;
              res += `${separator}"...":"${getItemCount(removedKeys)} not stringified"`;
            }
            stack.pop();
            return `{${res}}`;
          }
          case "number":
            return isFinite(value) ? String(value) : fail ? fail(value) : "null";
          case "boolean":
            return value === true ? "true" : "false";
          case "undefined":
            return void 0;
          case "bigint":
            if (bigint2) {
              return String(value);
            }
          // fallthrough
          default:
            return fail ? fail(value) : void 0;
        }
      }
      __name(stringifySimple, "stringifySimple");
      function stringify2(value, replacer, space) {
        if (arguments.length > 1) {
          let spacer = "";
          if (typeof space === "number") {
            spacer = " ".repeat(Math.min(space, 10));
          } else if (typeof space === "string") {
            spacer = space.slice(0, 10);
          }
          if (replacer != null) {
            if (typeof replacer === "function") {
              return stringifyFnReplacer("", { "": value }, [], replacer, spacer, "");
            }
            if (Array.isArray(replacer)) {
              return stringifyArrayReplacer("", value, [], getUniqueReplacerSet(replacer), spacer, "");
            }
          }
          if (spacer.length !== 0) {
            return stringifyIndent("", value, [], spacer, "");
          }
        }
        return stringifySimple("", value, []);
      }
      __name(stringify2, "stringify");
      return stringify2;
    }
    __name(configure2, "configure");
  }
});

// src/index.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../core/src/index.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../core/src/types.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../core/src/crypto.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../../node_modules/@noble/hashes/esm/sha256.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../../node_modules/@noble/hashes/esm/sha2.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../../node_modules/@noble/hashes/esm/_md.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../../node_modules/@noble/hashes/esm/utils.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
function isBytes(a) {
  return a instanceof Uint8Array || ArrayBuffer.isView(a) && a.constructor.name === "Uint8Array";
}
__name(isBytes, "isBytes");
function anumber(n) {
  if (!Number.isSafeInteger(n) || n < 0)
    throw new Error("positive integer expected, got " + n);
}
__name(anumber, "anumber");
function abytes(b, ...lengths) {
  if (!isBytes(b))
    throw new Error("Uint8Array expected");
  if (lengths.length > 0 && !lengths.includes(b.length))
    throw new Error("Uint8Array expected of length " + lengths + ", got length=" + b.length);
}
__name(abytes, "abytes");
function ahash(h) {
  if (typeof h !== "function" || typeof h.create !== "function")
    throw new Error("Hash should be wrapped by utils.createHasher");
  anumber(h.outputLen);
  anumber(h.blockLen);
}
__name(ahash, "ahash");
function aexists(instance, checkFinished = true) {
  if (instance.destroyed)
    throw new Error("Hash instance has been destroyed");
  if (checkFinished && instance.finished)
    throw new Error("Hash#digest() has already been called");
}
__name(aexists, "aexists");
function aoutput(out, instance) {
  abytes(out);
  const min = instance.outputLen;
  if (out.length < min) {
    throw new Error("digestInto() expects output buffer of length at least " + min);
  }
}
__name(aoutput, "aoutput");
function clean(...arrays) {
  for (let i = 0; i < arrays.length; i++) {
    arrays[i].fill(0);
  }
}
__name(clean, "clean");
function createView(arr) {
  return new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
}
__name(createView, "createView");
function rotr(word, shift) {
  return word << 32 - shift | word >>> shift;
}
__name(rotr, "rotr");
function utf8ToBytes(str) {
  if (typeof str !== "string")
    throw new Error("string expected");
  return new Uint8Array(new TextEncoder().encode(str));
}
__name(utf8ToBytes, "utf8ToBytes");
function toBytes(data) {
  if (typeof data === "string")
    data = utf8ToBytes(data);
  abytes(data);
  return data;
}
__name(toBytes, "toBytes");
var Hash = class {
  static {
    __name(this, "Hash");
  }
};
function createHasher(hashCons) {
  const hashC = /* @__PURE__ */ __name((msg) => hashCons().update(toBytes(msg)).digest(), "hashC");
  const tmp = hashCons();
  hashC.outputLen = tmp.outputLen;
  hashC.blockLen = tmp.blockLen;
  hashC.create = () => hashCons();
  return hashC;
}
__name(createHasher, "createHasher");

// ../../node_modules/@noble/hashes/esm/_md.js
function setBigUint64(view, byteOffset, value, isLE) {
  if (typeof view.setBigUint64 === "function")
    return view.setBigUint64(byteOffset, value, isLE);
  const _32n = BigInt(32);
  const _u32_max = BigInt(4294967295);
  const wh = Number(value >> _32n & _u32_max);
  const wl = Number(value & _u32_max);
  const h = isLE ? 4 : 0;
  const l = isLE ? 0 : 4;
  view.setUint32(byteOffset + h, wh, isLE);
  view.setUint32(byteOffset + l, wl, isLE);
}
__name(setBigUint64, "setBigUint64");
function Chi(a, b, c) {
  return a & b ^ ~a & c;
}
__name(Chi, "Chi");
function Maj(a, b, c) {
  return a & b ^ a & c ^ b & c;
}
__name(Maj, "Maj");
var HashMD = class extends Hash {
  static {
    __name(this, "HashMD");
  }
  constructor(blockLen, outputLen, padOffset, isLE) {
    super();
    this.finished = false;
    this.length = 0;
    this.pos = 0;
    this.destroyed = false;
    this.blockLen = blockLen;
    this.outputLen = outputLen;
    this.padOffset = padOffset;
    this.isLE = isLE;
    this.buffer = new Uint8Array(blockLen);
    this.view = createView(this.buffer);
  }
  update(data) {
    aexists(this);
    data = toBytes(data);
    abytes(data);
    const { view, buffer, blockLen } = this;
    const len = data.length;
    for (let pos = 0; pos < len; ) {
      const take = Math.min(blockLen - this.pos, len - pos);
      if (take === blockLen) {
        const dataView = createView(data);
        for (; blockLen <= len - pos; pos += blockLen)
          this.process(dataView, pos);
        continue;
      }
      buffer.set(data.subarray(pos, pos + take), this.pos);
      this.pos += take;
      pos += take;
      if (this.pos === blockLen) {
        this.process(view, 0);
        this.pos = 0;
      }
    }
    this.length += data.length;
    this.roundClean();
    return this;
  }
  digestInto(out) {
    aexists(this);
    aoutput(out, this);
    this.finished = true;
    const { buffer, view, blockLen, isLE } = this;
    let { pos } = this;
    buffer[pos++] = 128;
    clean(this.buffer.subarray(pos));
    if (this.padOffset > blockLen - pos) {
      this.process(view, 0);
      pos = 0;
    }
    for (let i = pos; i < blockLen; i++)
      buffer[i] = 0;
    setBigUint64(view, blockLen - 8, BigInt(this.length * 8), isLE);
    this.process(view, 0);
    const oview = createView(out);
    const len = this.outputLen;
    if (len % 4)
      throw new Error("_sha2: outputLen should be aligned to 32bit");
    const outLen = len / 4;
    const state = this.get();
    if (outLen > state.length)
      throw new Error("_sha2: outputLen bigger than state");
    for (let i = 0; i < outLen; i++)
      oview.setUint32(4 * i, state[i], isLE);
  }
  digest() {
    const { buffer, outputLen } = this;
    this.digestInto(buffer);
    const res = buffer.slice(0, outputLen);
    this.destroy();
    return res;
  }
  _cloneInto(to) {
    to || (to = new this.constructor());
    to.set(...this.get());
    const { blockLen, buffer, length, finished, destroyed, pos } = this;
    to.destroyed = destroyed;
    to.finished = finished;
    to.length = length;
    to.pos = pos;
    if (length % blockLen)
      to.buffer.set(buffer);
    return to;
  }
  clone() {
    return this._cloneInto();
  }
};
var SHA256_IV = /* @__PURE__ */ Uint32Array.from([
  1779033703,
  3144134277,
  1013904242,
  2773480762,
  1359893119,
  2600822924,
  528734635,
  1541459225
]);

// ../../node_modules/@noble/hashes/esm/sha2.js
var SHA256_K = /* @__PURE__ */ Uint32Array.from([
  1116352408,
  1899447441,
  3049323471,
  3921009573,
  961987163,
  1508970993,
  2453635748,
  2870763221,
  3624381080,
  310598401,
  607225278,
  1426881987,
  1925078388,
  2162078206,
  2614888103,
  3248222580,
  3835390401,
  4022224774,
  264347078,
  604807628,
  770255983,
  1249150122,
  1555081692,
  1996064986,
  2554220882,
  2821834349,
  2952996808,
  3210313671,
  3336571891,
  3584528711,
  113926993,
  338241895,
  666307205,
  773529912,
  1294757372,
  1396182291,
  1695183700,
  1986661051,
  2177026350,
  2456956037,
  2730485921,
  2820302411,
  3259730800,
  3345764771,
  3516065817,
  3600352804,
  4094571909,
  275423344,
  430227734,
  506948616,
  659060556,
  883997877,
  958139571,
  1322822218,
  1537002063,
  1747873779,
  1955562222,
  2024104815,
  2227730452,
  2361852424,
  2428436474,
  2756734187,
  3204031479,
  3329325298
]);
var SHA256_W = /* @__PURE__ */ new Uint32Array(64);
var SHA256 = class extends HashMD {
  static {
    __name(this, "SHA256");
  }
  constructor(outputLen = 32) {
    super(64, outputLen, 8, false);
    this.A = SHA256_IV[0] | 0;
    this.B = SHA256_IV[1] | 0;
    this.C = SHA256_IV[2] | 0;
    this.D = SHA256_IV[3] | 0;
    this.E = SHA256_IV[4] | 0;
    this.F = SHA256_IV[5] | 0;
    this.G = SHA256_IV[6] | 0;
    this.H = SHA256_IV[7] | 0;
  }
  get() {
    const { A, B, C: C2, D, E, F, G: G2, H } = this;
    return [A, B, C2, D, E, F, G2, H];
  }
  // prettier-ignore
  set(A, B, C2, D, E, F, G2, H) {
    this.A = A | 0;
    this.B = B | 0;
    this.C = C2 | 0;
    this.D = D | 0;
    this.E = E | 0;
    this.F = F | 0;
    this.G = G2 | 0;
    this.H = H | 0;
  }
  process(view, offset) {
    for (let i = 0; i < 16; i++, offset += 4)
      SHA256_W[i] = view.getUint32(offset, false);
    for (let i = 16; i < 64; i++) {
      const W15 = SHA256_W[i - 15];
      const W2 = SHA256_W[i - 2];
      const s0 = rotr(W15, 7) ^ rotr(W15, 18) ^ W15 >>> 3;
      const s1 = rotr(W2, 17) ^ rotr(W2, 19) ^ W2 >>> 10;
      SHA256_W[i] = s1 + SHA256_W[i - 7] + s0 + SHA256_W[i - 16] | 0;
    }
    let { A, B, C: C2, D, E, F, G: G2, H } = this;
    for (let i = 0; i < 64; i++) {
      const sigma1 = rotr(E, 6) ^ rotr(E, 11) ^ rotr(E, 25);
      const T1 = H + sigma1 + Chi(E, F, G2) + SHA256_K[i] + SHA256_W[i] | 0;
      const sigma0 = rotr(A, 2) ^ rotr(A, 13) ^ rotr(A, 22);
      const T2 = sigma0 + Maj(A, B, C2) | 0;
      H = G2;
      G2 = F;
      F = E;
      E = D + T1 | 0;
      D = C2;
      C2 = B;
      B = A;
      A = T1 + T2 | 0;
    }
    A = A + this.A | 0;
    B = B + this.B | 0;
    C2 = C2 + this.C | 0;
    D = D + this.D | 0;
    E = E + this.E | 0;
    F = F + this.F | 0;
    G2 = G2 + this.G | 0;
    H = H + this.H | 0;
    this.set(A, B, C2, D, E, F, G2, H);
  }
  roundClean() {
    clean(SHA256_W);
  }
  destroy() {
    this.set(0, 0, 0, 0, 0, 0, 0, 0);
    clean(this.buffer);
  }
};
var sha256 = /* @__PURE__ */ createHasher(() => new SHA256());

// ../../node_modules/@noble/hashes/esm/sha256.js
var sha2562 = sha256;

// ../../node_modules/@noble/hashes/esm/hmac.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var HMAC = class extends Hash {
  static {
    __name(this, "HMAC");
  }
  constructor(hash, _key) {
    super();
    this.finished = false;
    this.destroyed = false;
    ahash(hash);
    const key = toBytes(_key);
    this.iHash = hash.create();
    if (typeof this.iHash.update !== "function")
      throw new Error("Expected instance of class which extends utils.Hash");
    this.blockLen = this.iHash.blockLen;
    this.outputLen = this.iHash.outputLen;
    const blockLen = this.blockLen;
    const pad = new Uint8Array(blockLen);
    pad.set(key.length > blockLen ? hash.create().update(key).digest() : key);
    for (let i = 0; i < pad.length; i++)
      pad[i] ^= 54;
    this.iHash.update(pad);
    this.oHash = hash.create();
    for (let i = 0; i < pad.length; i++)
      pad[i] ^= 54 ^ 92;
    this.oHash.update(pad);
    clean(pad);
  }
  update(buf) {
    aexists(this);
    this.iHash.update(buf);
    return this;
  }
  digestInto(out) {
    aexists(this);
    abytes(out, this.outputLen);
    this.finished = true;
    this.iHash.digestInto(out);
    this.oHash.update(out);
    this.oHash.digestInto(out);
    this.destroy();
  }
  digest() {
    const out = new Uint8Array(this.oHash.outputLen);
    this.digestInto(out);
    return out;
  }
  _cloneInto(to) {
    to || (to = Object.create(Object.getPrototypeOf(this), {}));
    const { oHash, iHash, finished, destroyed, blockLen, outputLen } = this;
    to = to;
    to.finished = finished;
    to.destroyed = destroyed;
    to.blockLen = blockLen;
    to.outputLen = outputLen;
    to.oHash = oHash._cloneInto(to.oHash);
    to.iHash = iHash._cloneInto(to.iHash);
    return to;
  }
  clone() {
    return this._cloneInto();
  }
  destroy() {
    this.destroyed = true;
    this.oHash.destroy();
    this.iHash.destroy();
  }
};
var hmac = /* @__PURE__ */ __name((hash, key, message) => new HMAC(hash, key).update(message).digest(), "hmac");
hmac.create = (hash, key) => new HMAC(hash, key);

// ../../node_modules/@noble/secp256k1/index.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var secp256k1_CURVE = {
  p: 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2fn,
  n: 0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141n,
  h: 1n,
  a: 0n,
  b: 7n,
  Gx: 0x79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798n,
  Gy: 0x483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8n
};
var { p: P, n: N, Gx, Gy, b: _b } = secp256k1_CURVE;
var L = 32;
var L2 = 64;
var err = /* @__PURE__ */ __name((m = "") => {
  throw new Error(m);
}, "err");
var isBig = /* @__PURE__ */ __name((n) => typeof n === "bigint", "isBig");
var isStr = /* @__PURE__ */ __name((s) => typeof s === "string", "isStr");
var isBytes2 = /* @__PURE__ */ __name((a) => a instanceof Uint8Array || ArrayBuffer.isView(a) && a.constructor.name === "Uint8Array", "isBytes");
var abytes2 = /* @__PURE__ */ __name((a, l) => !isBytes2(a) || typeof l === "number" && l > 0 && a.length !== l ? err("Uint8Array expected") : a, "abytes");
var u8n = /* @__PURE__ */ __name((len) => new Uint8Array(len), "u8n");
var u8fr = /* @__PURE__ */ __name((buf) => Uint8Array.from(buf), "u8fr");
var padh = /* @__PURE__ */ __name((n, pad) => n.toString(16).padStart(pad, "0"), "padh");
var bytesToHex = /* @__PURE__ */ __name((b) => Array.from(abytes2(b)).map((e) => padh(e, 2)).join(""), "bytesToHex");
var C = { _0: 48, _9: 57, A: 65, F: 70, a: 97, f: 102 };
var _ch = /* @__PURE__ */ __name((ch) => {
  if (ch >= C._0 && ch <= C._9)
    return ch - C._0;
  if (ch >= C.A && ch <= C.F)
    return ch - (C.A - 10);
  if (ch >= C.a && ch <= C.f)
    return ch - (C.a - 10);
  return;
}, "_ch");
var hexToBytes = /* @__PURE__ */ __name((hex) => {
  const e = "hex invalid";
  if (!isStr(hex))
    return err(e);
  const hl = hex.length;
  const al = hl / 2;
  if (hl % 2)
    return err(e);
  const array = u8n(al);
  for (let ai = 0, hi = 0; ai < al; ai++, hi += 2) {
    const n1 = _ch(hex.charCodeAt(hi));
    const n2 = _ch(hex.charCodeAt(hi + 1));
    if (n1 === void 0 || n2 === void 0)
      return err(e);
    array[ai] = n1 * 16 + n2;
  }
  return array;
}, "hexToBytes");
var toU8 = /* @__PURE__ */ __name((a, len) => abytes2(isStr(a) ? hexToBytes(a) : u8fr(abytes2(a)), len), "toU8");
var cr = /* @__PURE__ */ __name(() => globalThis?.crypto, "cr");
var subtle = /* @__PURE__ */ __name(() => cr()?.subtle ?? err("crypto.subtle must be defined"), "subtle");
var concatBytes = /* @__PURE__ */ __name((...arrs) => {
  const r = u8n(arrs.reduce((sum, a) => sum + abytes2(a).length, 0));
  let pad = 0;
  arrs.forEach((a) => {
    r.set(a, pad);
    pad += a.length;
  });
  return r;
}, "concatBytes");
var randomBytes = /* @__PURE__ */ __name((len = L) => {
  const c = cr();
  return c.getRandomValues(u8n(len));
}, "randomBytes");
var big = BigInt;
var arange = /* @__PURE__ */ __name((n, min, max, msg = "bad number: out of range") => isBig(n) && min <= n && n < max ? n : err(msg), "arange");
var M = /* @__PURE__ */ __name((a, b = P) => {
  const r = a % b;
  return r >= 0n ? r : b + r;
}, "M");
var modN = /* @__PURE__ */ __name((a) => M(a, N), "modN");
var invert = /* @__PURE__ */ __name((num, md) => {
  if (num === 0n || md <= 0n)
    err("no inverse n=" + num + " mod=" + md);
  let a = M(num, md), b = md, x = 0n, y = 1n, u = 1n, v = 0n;
  while (a !== 0n) {
    const q = b / a, r = b % a;
    const m = x - u * q, n = y - v * q;
    b = a, a = r, x = u, y = v, u = m, v = n;
  }
  return b === 1n ? M(x, md) : err("no inverse");
}, "invert");
var callHash = /* @__PURE__ */ __name((name) => {
  const fn = etc[name];
  if (typeof fn !== "function")
    err("hashes." + name + " not set");
  return fn;
}, "callHash");
var apoint = /* @__PURE__ */ __name((p) => p instanceof Point ? p : err("Point expected"), "apoint");
var koblitz = /* @__PURE__ */ __name((x) => M(M(x * x) * x + _b), "koblitz");
var afield0 = /* @__PURE__ */ __name((n) => arange(n, 0n, P), "afield0");
var afield = /* @__PURE__ */ __name((n) => arange(n, 1n, P), "afield");
var agroup = /* @__PURE__ */ __name((n) => arange(n, 1n, N), "agroup");
var isEven = /* @__PURE__ */ __name((y) => (y & 1n) === 0n, "isEven");
var u8of = /* @__PURE__ */ __name((n) => Uint8Array.of(n), "u8of");
var getPrefix = /* @__PURE__ */ __name((y) => u8of(isEven(y) ? 2 : 3), "getPrefix");
var lift_x = /* @__PURE__ */ __name((x) => {
  const c = koblitz(afield(x));
  let r = 1n;
  for (let num = c, e = (P + 1n) / 4n; e > 0n; e >>= 1n) {
    if (e & 1n)
      r = r * num % P;
    num = num * num % P;
  }
  return M(r * r) === c ? r : err("sqrt invalid");
}, "lift_x");
var Point = class _Point {
  static {
    __name(this, "Point");
  }
  static BASE;
  static ZERO;
  px;
  py;
  pz;
  constructor(px, py, pz) {
    this.px = afield0(px);
    this.py = afield(py);
    this.pz = afield0(pz);
    Object.freeze(this);
  }
  /** Convert Uint8Array or hex string to Point. */
  static fromBytes(bytes) {
    abytes2(bytes);
    let p = void 0;
    const head = bytes[0];
    const tail = bytes.subarray(1);
    const x = sliceBytesNumBE(tail, 0, L);
    const len = bytes.length;
    if (len === L + 1 && [2, 3].includes(head)) {
      let y = lift_x(x);
      const evenY = isEven(y);
      const evenH = isEven(big(head));
      if (evenH !== evenY)
        y = M(-y);
      p = new _Point(x, y, 1n);
    }
    if (len === L2 + 1 && head === 4)
      p = new _Point(x, sliceBytesNumBE(tail, L, L2), 1n);
    return p ? p.assertValidity() : err("bad point: not on curve");
  }
  /** Equality check: compare points P&Q. */
  equals(other) {
    const { px: X1, py: Y1, pz: Z1 } = this;
    const { px: X2, py: Y2, pz: Z2 } = apoint(other);
    const X1Z2 = M(X1 * Z2);
    const X2Z1 = M(X2 * Z1);
    const Y1Z2 = M(Y1 * Z2);
    const Y2Z1 = M(Y2 * Z1);
    return X1Z2 === X2Z1 && Y1Z2 === Y2Z1;
  }
  is0() {
    return this.equals(I);
  }
  /** Flip point over y coordinate. */
  negate() {
    return new _Point(this.px, M(-this.py), this.pz);
  }
  /** Point doubling: P+P, complete formula. */
  double() {
    return this.add(this);
  }
  /**
   * Point addition: P+Q, complete, exception-free formula
   * (Renes-Costello-Batina, algo 1 of [2015/1060](https://eprint.iacr.org/2015/1060)).
   * Cost: `12M + 0S + 3*a + 3*b3 + 23add`.
   */
  // prettier-ignore
  add(other) {
    const { px: X1, py: Y1, pz: Z1 } = this;
    const { px: X2, py: Y2, pz: Z2 } = apoint(other);
    const a = 0n;
    const b = _b;
    let X3 = 0n, Y3 = 0n, Z3 = 0n;
    const b3 = M(b * 3n);
    let t0 = M(X1 * X2), t1 = M(Y1 * Y2), t2 = M(Z1 * Z2), t3 = M(X1 + Y1);
    let t4 = M(X2 + Y2);
    t3 = M(t3 * t4);
    t4 = M(t0 + t1);
    t3 = M(t3 - t4);
    t4 = M(X1 + Z1);
    let t5 = M(X2 + Z2);
    t4 = M(t4 * t5);
    t5 = M(t0 + t2);
    t4 = M(t4 - t5);
    t5 = M(Y1 + Z1);
    X3 = M(Y2 + Z2);
    t5 = M(t5 * X3);
    X3 = M(t1 + t2);
    t5 = M(t5 - X3);
    Z3 = M(a * t4);
    X3 = M(b3 * t2);
    Z3 = M(X3 + Z3);
    X3 = M(t1 - Z3);
    Z3 = M(t1 + Z3);
    Y3 = M(X3 * Z3);
    t1 = M(t0 + t0);
    t1 = M(t1 + t0);
    t2 = M(a * t2);
    t4 = M(b3 * t4);
    t1 = M(t1 + t2);
    t2 = M(t0 - t2);
    t2 = M(a * t2);
    t4 = M(t4 + t2);
    t0 = M(t1 * t4);
    Y3 = M(Y3 + t0);
    t0 = M(t5 * t4);
    X3 = M(t3 * X3);
    X3 = M(X3 - t0);
    t0 = M(t3 * t1);
    Z3 = M(t5 * Z3);
    Z3 = M(Z3 + t0);
    return new _Point(X3, Y3, Z3);
  }
  /**
   * Point-by-scalar multiplication. Scalar must be in range 1 <= n < CURVE.n.
   * Uses {@link wNAF} for base point.
   * Uses fake point to mitigate side-channel leakage.
   * @param n scalar by which point is multiplied
   * @param safe safe mode guards against timing attacks; unsafe mode is faster
   */
  multiply(n, safe = true) {
    if (!safe && n === 0n)
      return I;
    agroup(n);
    if (n === 1n)
      return this;
    if (this.equals(G))
      return wNAF(n).p;
    let p = I;
    let f = G;
    for (let d = this; n > 0n; d = d.double(), n >>= 1n) {
      if (n & 1n)
        p = p.add(d);
      else if (safe)
        f = f.add(d);
    }
    return p;
  }
  /** Convert point to 2d xy affine point. (X, Y, Z) ∋ (x=X/Z, y=Y/Z) */
  toAffine() {
    const { px: x, py: y, pz: z } = this;
    if (this.equals(I))
      return { x: 0n, y: 0n };
    if (z === 1n)
      return { x, y };
    const iz = invert(z, P);
    if (M(z * iz) !== 1n)
      err("inverse invalid");
    return { x: M(x * iz), y: M(y * iz) };
  }
  /** Checks if the point is valid and on-curve. */
  assertValidity() {
    const { x, y } = this.toAffine();
    afield(x);
    afield(y);
    return M(y * y) === koblitz(x) ? this : err("bad point: not on curve");
  }
  /** Converts point to 33/65-byte Uint8Array. */
  toBytes(isCompressed = true) {
    const { x, y } = this.assertValidity().toAffine();
    const x32b = numTo32b(x);
    if (isCompressed)
      return concatBytes(getPrefix(y), x32b);
    return concatBytes(u8of(4), x32b, numTo32b(y));
  }
  /** Create 3d xyz point from 2d xy. (0, 0) => (0, 1, 0), not (0, 0, 1) */
  static fromAffine(ap) {
    const { x, y } = ap;
    return x === 0n && y === 0n ? I : new _Point(x, y, 1n);
  }
  toHex(isCompressed) {
    return bytesToHex(this.toBytes(isCompressed));
  }
  static fromPrivateKey(k) {
    return G.multiply(toPrivScalar(k));
  }
  static fromHex(hex) {
    return _Point.fromBytes(toU8(hex));
  }
  get x() {
    return this.toAffine().x;
  }
  get y() {
    return this.toAffine().y;
  }
  toRawBytes(isCompressed) {
    return this.toBytes(isCompressed);
  }
};
var G = new Point(Gx, Gy, 1n);
var I = new Point(0n, 1n, 0n);
Point.BASE = G;
Point.ZERO = I;
var doubleScalarMulUns = /* @__PURE__ */ __name((R, u1, u2) => {
  return G.multiply(u1, false).add(R.multiply(u2, false)).assertValidity();
}, "doubleScalarMulUns");
var bytesToNumBE = /* @__PURE__ */ __name((b) => big("0x" + (bytesToHex(b) || "0")), "bytesToNumBE");
var sliceBytesNumBE = /* @__PURE__ */ __name((b, from, to) => bytesToNumBE(b.subarray(from, to)), "sliceBytesNumBE");
var B256 = 2n ** 256n;
var numTo32b = /* @__PURE__ */ __name((num) => hexToBytes(padh(arange(num, 0n, B256), L2)), "numTo32b");
var toPrivScalar = /* @__PURE__ */ __name((pr) => {
  const num = isBig(pr) ? pr : bytesToNumBE(toU8(pr, L));
  return arange(num, 1n, N, "private key invalid 3");
}, "toPrivScalar");
var highS = /* @__PURE__ */ __name((n) => n > N >> 1n, "highS");
var getPublicKey = /* @__PURE__ */ __name((privKey, isCompressed = true) => {
  return G.multiply(toPrivScalar(privKey)).toBytes(isCompressed);
}, "getPublicKey");
var Signature = class _Signature {
  static {
    __name(this, "Signature");
  }
  r;
  s;
  recovery;
  constructor(r, s, recovery) {
    this.r = agroup(r);
    this.s = agroup(s);
    if (recovery != null)
      this.recovery = recovery;
    Object.freeze(this);
  }
  /** Create signature from 64b compact (r || s) representation. */
  static fromBytes(b) {
    abytes2(b, L2);
    const r = sliceBytesNumBE(b, 0, L);
    const s = sliceBytesNumBE(b, L, L2);
    return new _Signature(r, s);
  }
  toBytes() {
    const { r, s } = this;
    return concatBytes(numTo32b(r), numTo32b(s));
  }
  /** Copy signature, with newly added recovery bit. */
  addRecoveryBit(bit) {
    return new _Signature(this.r, this.s, bit);
  }
  hasHighS() {
    return highS(this.s);
  }
  toCompactRawBytes() {
    return this.toBytes();
  }
  toCompactHex() {
    return bytesToHex(this.toBytes());
  }
  recoverPublicKey(msg) {
    return recoverPublicKey(this, msg);
  }
  static fromCompact(hex) {
    return _Signature.fromBytes(toU8(hex, L2));
  }
  assertValidity() {
    return this;
  }
  normalizeS() {
    const { r, s, recovery } = this;
    return highS(s) ? new _Signature(r, modN(-s), recovery) : this;
  }
};
var bits2int = /* @__PURE__ */ __name((bytes) => {
  const delta = bytes.length * 8 - 256;
  if (delta > 1024)
    err("msg invalid");
  const num = bytesToNumBE(bytes);
  return delta > 0 ? num >> big(delta) : num;
}, "bits2int");
var bits2int_modN = /* @__PURE__ */ __name((bytes) => modN(bits2int(abytes2(bytes))), "bits2int_modN");
var signOpts = { lowS: true };
var veriOpts = { lowS: true };
var prepSig = /* @__PURE__ */ __name((msgh, priv, opts = signOpts) => {
  if (["der", "recovered", "canonical"].some((k) => k in opts))
    err("option not supported");
  let { lowS, extraEntropy } = opts;
  if (lowS == null)
    lowS = true;
  const i2o = numTo32b;
  const h1i = bits2int_modN(toU8(msgh));
  const h1o = i2o(h1i);
  const d = toPrivScalar(priv);
  const seed = [i2o(d), h1o];
  if (extraEntropy)
    seed.push(extraEntropy === true ? randomBytes(L) : toU8(extraEntropy));
  const m = h1i;
  const k2sig = /* @__PURE__ */ __name((kBytes) => {
    const k = bits2int(kBytes);
    if (!(1n <= k && k < N))
      return;
    const q = G.multiply(k).toAffine();
    const r = modN(q.x);
    if (r === 0n)
      return;
    const ik = invert(k, N);
    const s = modN(ik * modN(m + modN(d * r)));
    if (s === 0n)
      return;
    let normS = s;
    let recovery = (q.x === r ? 0 : 2) | Number(q.y & 1n);
    if (lowS && highS(s)) {
      normS = modN(-s);
      recovery ^= 1;
    }
    return new Signature(r, normS, recovery);
  }, "k2sig");
  return { seed: concatBytes(...seed), k2sig };
}, "prepSig");
var hmacDrbg = /* @__PURE__ */ __name((asynchronous) => {
  let v = u8n(L);
  let k = u8n(L);
  let i = 0;
  const NULL = u8n(0);
  const reset = /* @__PURE__ */ __name(() => {
    v.fill(1);
    k.fill(0);
    i = 0;
  }, "reset");
  const max = 1e3;
  const _e = "drbg: tried 1000 values";
  if (asynchronous) {
    const h = /* @__PURE__ */ __name((...b) => etc.hmacSha256Async(k, v, ...b), "h");
    const reseed = /* @__PURE__ */ __name(async (seed = NULL) => {
      k = await h(u8of(0), seed);
      v = await h();
      if (seed.length === 0)
        return;
      k = await h(u8of(1), seed);
      v = await h();
    }, "reseed");
    const gen = /* @__PURE__ */ __name(async () => {
      if (i++ >= max)
        err(_e);
      v = await h();
      return v;
    }, "gen");
    return async (seed, pred) => {
      reset();
      await reseed(seed);
      let res = void 0;
      while (!(res = pred(await gen())))
        await reseed();
      reset();
      return res;
    };
  } else {
    const h = /* @__PURE__ */ __name((...b) => callHash("hmacSha256Sync")(k, v, ...b), "h");
    const reseed = /* @__PURE__ */ __name((seed = NULL) => {
      k = h(u8of(0), seed);
      v = h();
      if (seed.length === 0)
        return;
      k = h(u8of(1), seed);
      v = h();
    }, "reseed");
    const gen = /* @__PURE__ */ __name(() => {
      if (i++ >= max)
        err(_e);
      v = h();
      return v;
    }, "gen");
    return (seed, pred) => {
      reset();
      reseed(seed);
      let res = void 0;
      while (!(res = pred(gen())))
        reseed();
      reset();
      return res;
    };
  }
}, "hmacDrbg");
var signAsync = /* @__PURE__ */ __name(async (msgh, priv, opts = signOpts) => {
  const { seed, k2sig } = prepSig(msgh, priv, opts);
  const sig = await hmacDrbg(true)(seed, k2sig);
  return sig;
}, "signAsync");
var sign = /* @__PURE__ */ __name((msgh, priv, opts = signOpts) => {
  const { seed, k2sig } = prepSig(msgh, priv, opts);
  const sig = hmacDrbg(false)(seed, k2sig);
  return sig;
}, "sign");
var verify = /* @__PURE__ */ __name((sig, msgh, pub, opts = veriOpts) => {
  let { lowS } = opts;
  if (lowS == null)
    lowS = true;
  if ("strict" in opts)
    err("option not supported");
  let sigg;
  const rs = sig && typeof sig === "object" && "r" in sig;
  if (!rs && toU8(sig).length !== L2)
    err("signature must be 64 bytes");
  try {
    sigg = rs ? new Signature(sig.r, sig.s) : Signature.fromCompact(sig);
    const h = bits2int_modN(toU8(msgh));
    const P2 = Point.fromBytes(toU8(pub));
    const { r, s } = sigg;
    if (lowS && highS(s))
      return false;
    const is = invert(s, N);
    const u1 = modN(h * is);
    const u2 = modN(r * is);
    const R = doubleScalarMulUns(P2, u1, u2).toAffine();
    const v = modN(R.x);
    return v === r;
  } catch (error) {
    return false;
  }
}, "verify");
var recoverPublicKey = /* @__PURE__ */ __name((sig, msgh) => {
  const { r, s, recovery } = sig;
  if (![0, 1, 2, 3].includes(recovery))
    err("recovery id invalid");
  const h = bits2int_modN(toU8(msgh, L));
  const radj = recovery === 2 || recovery === 3 ? r + N : r;
  afield(radj);
  const head = getPrefix(big(recovery));
  const Rb = concatBytes(head, numTo32b(radj));
  const R = Point.fromBytes(Rb);
  const ir = invert(radj, N);
  const u1 = modN(-h * ir);
  const u2 = modN(s * ir);
  return doubleScalarMulUns(R, u1, u2);
}, "recoverPublicKey");
var hashToPrivateKey = /* @__PURE__ */ __name((hash) => {
  hash = toU8(hash);
  if (hash.length < L + 8 || hash.length > 1024)
    err("expected 40-1024b");
  const num = M(bytesToNumBE(hash), N - 1n);
  return numTo32b(num + 1n);
}, "hashToPrivateKey");
var randomPrivateKey = /* @__PURE__ */ __name(() => hashToPrivateKey(randomBytes(L + 16)), "randomPrivateKey");
var _sha = "SHA-256";
var etc = {
  hexToBytes,
  bytesToHex,
  concatBytes,
  bytesToNumberBE: bytesToNumBE,
  numberToBytesBE: numTo32b,
  mod: M,
  invert,
  // math utilities
  hmacSha256Async: /* @__PURE__ */ __name(async (key, ...msgs) => {
    const s = subtle();
    const name = "HMAC";
    const k = await s.importKey("raw", key, { name, hash: { name: _sha } }, false, ["sign"]);
    return u8n(await s.sign(name, k, concatBytes(...msgs)));
  }, "hmacSha256Async"),
  hmacSha256Sync: void 0,
  // For TypeScript. Actual logic is below
  hashToPrivateKey,
  randomBytes
};
var utils = {
  normPrivateKeyToScalar: toPrivScalar,
  isValidPrivateKey: /* @__PURE__ */ __name((key) => {
    try {
      return !!toPrivScalar(key);
    } catch (e) {
      return false;
    }
  }, "isValidPrivateKey"),
  randomPrivateKey,
  precompute: /* @__PURE__ */ __name((w = 8, p = G) => {
    p.multiply(3n);
    w;
    return p;
  }, "precompute")
};
var W = 8;
var scalarBits = 256;
var pwindows = Math.ceil(scalarBits / W) + 1;
var pwindowSize = 2 ** (W - 1);
var precompute = /* @__PURE__ */ __name(() => {
  const points = [];
  let p = G;
  let b = p;
  for (let w = 0; w < pwindows; w++) {
    b = p;
    points.push(b);
    for (let i = 1; i < pwindowSize; i++) {
      b = b.add(p);
      points.push(b);
    }
    p = b.double();
  }
  return points;
}, "precompute");
var Gpows = void 0;
var ctneg = /* @__PURE__ */ __name((cnd, p) => {
  const n = p.negate();
  return cnd ? n : p;
}, "ctneg");
var wNAF = /* @__PURE__ */ __name((n) => {
  const comp = Gpows || (Gpows = precompute());
  let p = I;
  let f = G;
  const pow_2_w = 2 ** W;
  const maxNum = pow_2_w;
  const mask = big(pow_2_w - 1);
  const shiftBy = big(W);
  for (let w = 0; w < pwindows; w++) {
    let wbits = Number(n & mask);
    n >>= shiftBy;
    if (wbits > pwindowSize) {
      wbits -= maxNum;
      n += 1n;
    }
    const off2 = w * pwindowSize;
    const offF = off2;
    const offP = off2 + Math.abs(wbits) - 1;
    const isEven2 = w % 2 !== 0;
    const isNeg = wbits < 0;
    if (wbits === 0) {
      f = f.add(ctneg(isEven2, comp[offF]));
    } else {
      p = p.add(ctneg(isNeg, comp[offP]));
    }
  }
  return { p, f };
}, "wNAF");

// ../core/src/crypto.ts
import { ECDH, createPublicKey, createVerify, webcrypto } from "node:crypto";

// ../../node_modules/safe-stable-stringify/esm/wrapper.js
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var import__ = __toESM(require_safe_stable_stringify(), 1);
var configure = import__.default.configure;
var wrapper_default = import__.default;

// ../core/src/crypto.ts
var textEncoder = new TextEncoder();
var textDecoder = new TextDecoder();
if (!etc.hmacSha256Sync) {
  etc.hmacSha256Sync = (key, ...msgs) => hmac(sha2562, key, etc.concatBytes(...msgs));
}
function hashObject(obj) {
  const bytes = textEncoder.encode(wrapper_default(obj));
  const h = sha2562(bytes);
  return Buffer.from(h).toString("hex");
}
__name(hashObject, "hashObject");
async function sign2(privKey, msgHashHex) {
  if (typeof process !== "undefined" && Boolean(process.versions?.node)) {
    const sig2 = sign(msgHashHex, privKey);
    return sig2.toCompactHex();
  }
  const sig = await signAsync(msgHashHex, privKey);
  return sig.toCompactHex();
}
__name(sign2, "sign");
function verify2(pubKeyHex, msgHashHex, sigHex) {
  try {
    return verify(sigHex, msgHashHex, pubKeyHex);
  } catch (e) {
    return false;
  }
}
__name(verify2, "verify");
var secp256k1SpkiPrefix = Buffer.from("3056301006072a8648ce3d020106052b8104000a034200", "hex");
var nativePublicKeyCache = /* @__PURE__ */ new Map();
var nativePublicKeyCacheLimit = 4096;
function compactSignatureToDer(sigHex) {
  const bytes = Buffer.from(sigHex, "hex");
  if (bytes.length !== 64) {
    return void 0;
  }
  const derInt = /* @__PURE__ */ __name((input) => {
    let value = input;
    while (value.length > 1 && value[0] === 0 && (value[1] & 128) === 0) {
      value = value.subarray(1);
    }
    if ((value[0] & 128) !== 0) {
      value = Buffer.concat([Buffer.from([0]), value]);
    }
    return Buffer.concat([Buffer.from([2, value.length]), value]);
  }, "derInt");
  const r = derInt(bytes.subarray(0, 32));
  const s = derInt(bytes.subarray(32, 64));
  return Buffer.concat([Buffer.from([48, r.length + s.length]), r, s]);
}
__name(compactSignatureToDer, "compactSignatureToDer");
function nativeSecp256k1PublicKey(pubKeyHex) {
  const cached = nativePublicKeyCache.get(pubKeyHex);
  if (cached) {
    return cached;
  }
  const source = Buffer.from(pubKeyHex, "hex");
  const uncompressed = source.length === 65 && source[0] === 4 ? source : ECDH.convertKey(source, "secp256k1", void 0, void 0, "uncompressed");
  const key = createPublicKey({
    key: Buffer.concat([secp256k1SpkiPrefix, Buffer.from(uncompressed)]),
    format: "der",
    type: "spki"
  });
  nativePublicKeyCache.set(pubKeyHex, key);
  if (nativePublicKeyCache.size > nativePublicKeyCacheLimit) {
    const oldestKey = nativePublicKeyCache.keys().next().value;
    if (oldestKey) {
      nativePublicKeyCache.delete(oldestKey);
    }
  }
  return key;
}
__name(nativeSecp256k1PublicKey, "nativeSecp256k1PublicKey");
function verifyObject(pubKeyHex, obj, sigHex) {
  const canonical = wrapper_default(obj) ?? "";
  try {
    const derSignature = compactSignatureToDer(sigHex);
    if (derSignature) {
      const verifier = createVerify("sha256");
      verifier.update(canonical, "utf8");
      verifier.end();
      return verifier.verify(nativeSecp256k1PublicKey(pubKeyHex), derSignature);
    }
  } catch {
  }
  return verify2(pubKeyHex, hashObject(obj), sigHex);
}
__name(verifyObject, "verifyObject");
function getPublicKey2(privKey) {
  return Buffer.from(getPublicKey(privKey, true)).toString("hex");
}
__name(getPublicKey2, "getPublicKey");
function generatePrivateKey() {
  return utils.randomPrivateKey();
}
__name(generatePrivateKey, "generatePrivateKey");

// ../core/src/log.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
function computeEventId(unsigned) {
  return hashObject({
    seq: unsigned.seq,
    prevHash: unsigned.prevHash,
    createdAt: unsigned.createdAt,
    author: unsigned.author,
    body: unsigned.body
  });
}
__name(computeEventId, "computeEventId");

// ../core/src/relay_head.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
function normalizeNullableHash(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}
__name(normalizeNullableHash, "normalizeNullableHash");
function relayHeadSigningPayload(head) {
  return {
    protocol: "cgp/0.1",
    relayId: head.relayId,
    relayPublicKey: head.relayPublicKey,
    guildId: head.guildId,
    headSeq: head.headSeq,
    headHash: normalizeNullableHash(head.headHash),
    prevHash: normalizeNullableHash(head.prevHash),
    checkpointSeq: typeof head.checkpointSeq === "number" ? head.checkpointSeq : null,
    checkpointHash: normalizeNullableHash(head.checkpointHash),
    observedAt: head.observedAt
  };
}
__name(relayHeadSigningPayload, "relayHeadSigningPayload");
function relayHeadId(head) {
  return hashObject(relayHeadSigningPayload(head));
}
__name(relayHeadId, "relayHeadId");

// ../core/src/state.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
function createInitialState(event) {
  if (event.body.type !== "GUILD_CREATE") {
    throw new Error("First event must be GUILD_CREATE");
  }
  const body = event.body;
  return {
    guildId: body.guildId,
    name: body.name,
    description: body.description,
    ownerId: event.author,
    channels: /* @__PURE__ */ new Map(),
    roles: /* @__PURE__ */ new Map(),
    members: /* @__PURE__ */ new Map([[event.author, { userId: event.author, roles: /* @__PURE__ */ new Set(["owner"]), joinedAt: event.createdAt }]]),
    bans: /* @__PURE__ */ new Map(),
    messages: /* @__PURE__ */ new Map(),
    appObjects: /* @__PURE__ */ new Map(),
    createdAt: event.createdAt,
    headSeq: event.seq,
    headHash: event.id,
    access: body.access || "public",
    policies: body.policies || {}
  };
}
__name(createInitialState, "createInitialState");
function serializeState(state) {
  return {
    guildId: state.guildId,
    name: state.name,
    description: state.description || "",
    ownerId: state.ownerId,
    channels: Array.from(state.channels.entries()),
    members: Array.from(state.members.entries()).map(([id, member]) => [
      id,
      {
        ...member,
        roles: Array.from(member.roles)
      }
    ]),
    roles: Array.from(state.roles.entries()),
    bans: Array.from(state.bans.entries()),
    messages: Array.from(state.messages.entries()),
    appObjects: Array.from(state.appObjects.entries()),
    access: state.access,
    policies: state.policies
  };
}
__name(serializeState, "serializeState");
function deserializeState(serialized, headSeq, headHash, createdAt) {
  const members = /* @__PURE__ */ new Map();
  for (const [id, sMember] of serialized.members) {
    members.set(id, {
      ...sMember,
      roles: new Set(sMember.roles)
    });
  }
  return {
    guildId: serialized.guildId,
    name: serialized.name,
    description: serialized.description,
    ownerId: serialized.ownerId,
    channels: new Map(serialized.channels),
    members,
    roles: new Map(serialized.roles),
    bans: new Map(serialized.bans),
    messages: new Map(serialized.messages ?? []),
    appObjects: new Map(serialized.appObjects ?? []),
    headSeq,
    headHash,
    createdAt,
    access: serialized.access,
    policies: serialized.policies || {}
  };
}
__name(deserializeState, "deserializeState");
function checkpointStateRoot(state) {
  return hashObject(state);
}
__name(checkpointStateRoot, "checkpointStateRoot");
function deserializeCheckpointState(event) {
  const body = event.body;
  if (body.type !== "CHECKPOINT") {
    throw new Error("Event is not a checkpoint");
  }
  if (body.guildId !== event.body.guildId) {
    throw new Error("Checkpoint guildId mismatch");
  }
  if (body.state.guildId !== body.guildId) {
    throw new Error("Checkpoint state guildId mismatch");
  }
  if (typeof body.seq === "number" && body.seq !== event.seq) {
    throw new Error("Checkpoint body seq must match event seq");
  }
  if (checkpointStateRoot(body.state) !== body.rootHash) {
    throw new Error("Checkpoint rootHash does not match state");
  }
  return deserializeState(body.state, event.seq, event.id, event.createdAt);
}
__name(deserializeCheckpointState, "deserializeCheckpointState");
function rebuildStateFromEvents(events) {
  if (events.length === 0) {
    throw new Error("Cannot rebuild state from an empty event list");
  }
  let checkpointIndex = -1;
  let state;
  for (let index = events.length - 1; index >= 0; index--) {
    if (events[index].body.type !== "CHECKPOINT") {
      continue;
    }
    try {
      state = deserializeCheckpointState(events[index]);
      checkpointIndex = index;
      break;
    } catch {
    }
  }
  const startIndex = checkpointIndex >= 0 ? checkpointIndex + 1 : 1;
  if (!state) {
    state = createInitialState(events[0]);
  }
  for (let i = startIndex; i < events.length; i++) {
    state = applyEvent(state, events[i], { mutable: true });
  }
  return {
    state,
    startIndex,
    checkpointEvent: checkpointIndex >= 0 ? events[checkpointIndex] : void 0
  };
}
__name(rebuildStateFromEvents, "rebuildStateFromEvents");
function applyEvent(state, event, options = {}) {
  const mutable = options.mutable === true;
  const newState = mutable ? state : { ...state };
  const ensureChannels = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.channels;
    if (newState.channels === state.channels) {
      newState.channels = new Map(state.channels);
    }
    return newState.channels;
  }, "ensureChannels");
  const ensureRoles = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.roles;
    if (newState.roles === state.roles) {
      newState.roles = new Map(state.roles);
    }
    return newState.roles;
  }, "ensureRoles");
  const ensureMembers = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.members;
    if (newState.members === state.members) {
      newState.members = new Map(state.members);
    }
    return newState.members;
  }, "ensureMembers");
  const ensureBans = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.bans;
    if (newState.bans === state.bans) {
      newState.bans = new Map(state.bans);
    }
    return newState.bans;
  }, "ensureBans");
  const ensureMessages = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.messages;
    if (newState.messages === state.messages) {
      newState.messages = new Map(state.messages);
    }
    return newState.messages;
  }, "ensureMessages");
  const ensureAppObjects = /* @__PURE__ */ __name(() => {
    if (mutable) return newState.appObjects;
    if (newState.appObjects === state.appObjects) {
      newState.appObjects = new Map(state.appObjects);
    }
    return newState.appObjects;
  }, "ensureAppObjects");
  newState.headSeq = event.seq;
  newState.headHash = event.id;
  newState.access = state.access;
  newState.policies = state.policies;
  const body = event.body;
  const bodyRecord = body;
  switch (bodyRecord.type) {
    case "GUILD_UPDATE": {
      if (typeof bodyRecord.name === "string" && bodyRecord.name.trim()) {
        newState.name = bodyRecord.name;
      }
      if (typeof bodyRecord.description === "string") {
        newState.description = bodyRecord.description;
      }
      if (bodyRecord.access === "public" || bodyRecord.access === "private") {
        newState.access = bodyRecord.access;
      }
      if (bodyRecord.policies && typeof bodyRecord.policies === "object") {
        newState.policies = {
          ...newState.policies,
          ...bodyRecord.policies
        };
      }
      break;
    }
    case "CHANNEL_CREATE": {
      const b = body;
      ensureChannels().set(b.channelId, {
        id: b.channelId,
        name: b.name,
        kind: b.kind,
        retention: b.retention,
        categoryId: b.categoryId,
        description: b.description,
        topic: b.topic,
        position: b.position,
        permissionOverwrites: Array.isArray(b.permissionOverwrites) ? b.permissionOverwrites : void 0
      });
      break;
    }
    case "CHANNEL_UPSERT": {
      if (typeof bodyRecord.channelId === "string" && bodyRecord.channelId.trim()) {
        const current = state.channels.get(bodyRecord.channelId);
        ensureChannels().set(bodyRecord.channelId, {
          ...current ?? {
            id: bodyRecord.channelId,
            name: bodyRecord.channelId,
            kind: "text"
          },
          id: bodyRecord.channelId,
          name: typeof bodyRecord.name === "string" && bodyRecord.name.trim() ? bodyRecord.name : current?.name ?? bodyRecord.channelId,
          kind: bodyRecord.kind || current?.kind || "text",
          retention: bodyRecord.retention ?? current?.retention,
          categoryId: bodyRecord.categoryId,
          description: bodyRecord.description,
          topic: bodyRecord.topic,
          position: bodyRecord.position,
          permissionOverwrites: Array.isArray(bodyRecord.permissionOverwrites) ? bodyRecord.permissionOverwrites : current?.permissionOverwrites
        });
      }
      break;
    }
    case "CHANNEL_DELETE": {
      if (typeof bodyRecord.channelId === "string") {
        ensureChannels().delete(bodyRecord.channelId);
        for (const [messageId, message] of state.messages) {
          if (message.channelId === bodyRecord.channelId) {
            ensureMessages().set(messageId, {
              ...message,
              deleted: true
            });
          }
        }
      }
      break;
    }
    case "ROLE_UPSERT": {
      if (typeof bodyRecord.roleId === "string" && bodyRecord.roleId.trim()) {
        const current = state.roles.get(bodyRecord.roleId);
        ensureRoles().set(bodyRecord.roleId, {
          ...current ?? {
            id: bodyRecord.roleId,
            name: bodyRecord.roleId,
            permissions: []
          },
          id: bodyRecord.roleId,
          name: typeof bodyRecord.name === "string" && bodyRecord.name.trim() ? bodyRecord.name : current?.name ?? bodyRecord.roleId,
          permissions: Array.isArray(bodyRecord.permissions) ? bodyRecord.permissions.filter((permission2) => typeof permission2 === "string") : current?.permissions ?? [],
          color: bodyRecord.color,
          icon: bodyRecord.icon,
          position: bodyRecord.position,
          mentionable: bodyRecord.mentionable,
          hoist: bodyRecord.hoist,
          managed: bodyRecord.managed
        });
      }
      break;
    }
    case "ROLE_DELETE": {
      if (typeof bodyRecord.roleId === "string") {
        ensureRoles().delete(bodyRecord.roleId);
        for (const [userId, member] of state.members) {
          if (member.roles.has(bodyRecord.roleId)) {
            ensureMembers().set(userId, {
              ...member,
              roles: new Set([...member.roles].filter((roleId) => roleId !== bodyRecord.roleId))
            });
          }
        }
      }
      break;
    }
    case "ROLE_ASSIGN": {
      const b = body;
      const current = state.members.get(b.userId);
      const member = current ? { ...current, roles: new Set(current.roles) } : { userId: b.userId, roles: /* @__PURE__ */ new Set(), joinedAt: event.createdAt };
      member.roles.add(b.roleId);
      ensureMembers().set(b.userId, member);
      break;
    }
    case "ROLE_REVOKE": {
      const b = body;
      const current = state.members.get(b.userId);
      if (current) {
        const member = { ...current, roles: new Set(current.roles) };
        member.roles.delete(b.roleId);
        ensureMembers().set(b.userId, member);
      }
      break;
    }
    case "BAN_USER": {
      const b = body;
      ensureBans().set(b.userId, {
        userId: b.userId,
        reason: b.reason,
        bannedAt: event.createdAt
      });
      ensureMembers().delete(b.userId);
      break;
    }
    case "BAN_ADD": {
      ensureBans().set(bodyRecord.userId, {
        userId: bodyRecord.userId,
        reason: bodyRecord.reason,
        expiresAt: bodyRecord.expiresAt,
        bannedAt: event.createdAt
      });
      ensureMembers().delete(bodyRecord.userId);
      break;
    }
    case "UNBAN_USER": {
      const b = body;
      ensureBans().delete(b.userId);
      break;
    }
    case "BAN_REMOVE": {
      ensureBans().delete(bodyRecord.userId);
      break;
    }
    case "MEMBER_KICK": {
      ensureMembers().delete(bodyRecord.userId);
      break;
    }
    case "MESSAGE": {
      const b = body;
      ensureMessages().set(b.messageId || event.id, {
        channelId: b.channelId,
        authorId: event.author,
        eventId: event.id,
        seq: event.seq
      });
      break;
    }
    case "REACTION_ADD": {
      const reaction = typeof bodyRecord.reaction === "string" ? bodyRecord.reaction.trim() : "";
      const message = state.messages.get(bodyRecord.messageId);
      if (reaction && message) {
        const reactions = { ...message.reactions ?? {} };
        const users = new Set(reactions[reaction] ?? []);
        users.add(event.author);
        reactions[reaction] = Array.from(users).sort();
        ensureMessages().set(bodyRecord.messageId, {
          ...message,
          reactions
        });
      }
      break;
    }
    case "REACTION_REMOVE": {
      const reaction = typeof bodyRecord.reaction === "string" ? bodyRecord.reaction.trim() : "";
      const message = state.messages.get(bodyRecord.messageId);
      if (reaction && message?.reactions?.[reaction]) {
        const reactions = { ...message.reactions };
        const removeUserId = typeof bodyRecord.userId === "string" && bodyRecord.userId.trim() ? bodyRecord.userId : event.author;
        const users = reactions[reaction].filter((userId) => userId !== removeUserId);
        if (users.length > 0) {
          reactions[reaction] = users;
        } else {
          delete reactions[reaction];
        }
        ensureMessages().set(bodyRecord.messageId, {
          ...message,
          reactions: Object.keys(reactions).length > 0 ? reactions : void 0
        });
      }
      break;
    }
    case "DELETE_MESSAGE": {
      const b = body;
      const message = state.messages.get(b.messageId);
      if (message) {
        ensureMessages().set(b.messageId, {
          ...message,
          deleted: true
        });
      }
      break;
    }
    case "APP_OBJECT_UPSERT": {
      if (typeof bodyRecord.namespace === "string" && typeof bodyRecord.objectType === "string" && typeof bodyRecord.objectId === "string") {
        const target = bodyRecord.target && typeof bodyRecord.target === "object" ? bodyRecord.target : void 0;
        const key = appObjectStateKey(bodyRecord.namespace, bodyRecord.objectType, bodyRecord.objectId);
        ensureAppObjects().set(key, {
          namespace: bodyRecord.namespace,
          objectType: bodyRecord.objectType,
          objectId: bodyRecord.objectId,
          channelId: bodyRecord.channelId ?? target?.channelId,
          target,
          value: bodyRecord.value,
          authorId: event.author,
          updatedAt: event.createdAt
        });
      }
      break;
    }
    case "APP_OBJECT_DELETE": {
      if (typeof bodyRecord.namespace === "string" && typeof bodyRecord.objectType === "string" && typeof bodyRecord.objectId === "string") {
        ensureAppObjects().delete(appObjectStateKey(bodyRecord.namespace, bodyRecord.objectType, bodyRecord.objectId));
      }
      break;
    }
    case "EPHEMERAL_POLICY_UPDATE": {
      const b = body;
      const channel2 = state.channels.get(b.channelId);
      if (channel2) {
        ensureChannels().set(b.channelId, {
          ...channel2,
          retention: b.retention
        });
      }
      break;
    }
    case "MEMBER_UPDATE": {
      const b = body;
      const targetUserId = bodyRecord.userId || event.author;
      const current = state.members.get(targetUserId);
      const member = current ? { ...current, roles: new Set(current.roles) } : { userId: targetUserId, roles: /* @__PURE__ */ new Set(), joinedAt: event.createdAt };
      if (b.nickname !== void 0) member.nickname = b.nickname;
      if (b.avatar !== void 0) member.avatar = b.avatar;
      if (b.banner !== void 0) member.banner = b.banner;
      if (b.bio !== void 0) member.bio = b.bio;
      if (Array.isArray(bodyRecord.roleIds)) member.roles = new Set(bodyRecord.roleIds.filter((roleId) => typeof roleId === "string"));
      if (bodyRecord.timedOutUntil !== void 0) member.timedOutUntil = bodyRecord.timedOutUntil;
      if (bodyRecord.isMuted !== void 0) member.isMuted = Boolean(bodyRecord.isMuted);
      if (bodyRecord.isDeafened !== void 0) member.isDeafened = Boolean(bodyRecord.isDeafened);
      ensureMembers().set(targetUserId, member);
      break;
    }
    case "CHECKPOINT": {
      const b = body;
      break;
    }
  }
  return newState;
}
__name(applyEvent, "applyEvent");
function appObjectStateKey(namespace, objectType, objectId) {
  return `${namespace}\0${objectType}\0${objectId}`;
}
__name(appObjectStateKey, "appObjectStateKey");

// ../core/src/validation.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var ADMIN_EVENT_TYPES = /* @__PURE__ */ new Map([
  ["GUILD_UPDATE", "guild"],
  ["CATEGORY_UPSERT", "channels"],
  ["CATEGORY_DELETE", "channels"],
  ["CHANNEL_UPSERT", "channels"],
  ["CHANNEL_DELETE", "channels"],
  ["EVENT_UPSERT", "events"],
  ["EVENT_DELETE", "events"],
  ["TEMPLATE_UPSERT", "guild"],
  ["TEMPLATE_DELETE", "guild"],
  ["INVITE_CREATE", "invites"],
  ["INVITE_REVOKE", "invites"]
]);
var CHANNEL_PARTICIPATION_EVENT_TYPES = /* @__PURE__ */ new Set([
  "REACTION_ADD",
  "REACTION_REMOVE",
  "DM_OPEN",
  "CALL_EVENT",
  "THREAD_UPSERT",
  "THREAD_DELETE"
]);
function assertCanParticipateInGuild(state, author) {
  if (state.bans.has(author)) {
    throw new Error(`User ${author} is banned`);
  }
  if ((state.access === "private" || state.policies.posting === "members") && !state.members.has(author)) {
    throw new Error(
      `User ${author} is not allowed to publish without guild membership.`
    );
  }
}
__name(assertCanParticipateInGuild, "assertCanParticipateInGuild");
function assertIsMember(state, author) {
  if (state.bans.has(author)) {
    throw new Error(`User ${author} is banned`);
  }
  if (!state.members.has(author)) {
    throw new Error(`User ${author} is not a member.`);
  }
}
__name(assertIsMember, "assertIsMember");
function assertChannelExists(state, channelId) {
  const channel2 = state.channels.get(channelId);
  if (!channel2) {
    throw new Error(`Channel ${channelId} does not exist`);
  }
  return channel2;
}
__name(assertChannelExists, "assertChannelExists");
function normalizePermission(value) {
  return value.replace(/[\s_-]+/g, "").toLowerCase();
}
__name(normalizePermission, "normalizePermission");
function permissionSetForMember(state, author) {
  const permissions = /* @__PURE__ */ new Set();
  const member = state.members.get(author);
  if (!member) {
    return permissions;
  }
  for (const roleId of member.roles) {
    permissions.add(normalizePermission(roleId));
    const role = state.roles.get(roleId);
    for (const permission2 of role?.permissions ?? []) {
      permissions.add(normalizePermission(permission2));
    }
  }
  return permissions;
}
__name(permissionSetForMember, "permissionSetForMember");
function channelBasePermissions(state, author) {
  const permissions = /* @__PURE__ */ new Set([
    "viewchannels",
    "sendmessages",
    "connect",
    "speak"
  ]);
  const member = state.members.get(author);
  if (state.bans.has(author)) {
    return /* @__PURE__ */ new Set();
  }
  if (state.access === "private" && !member) {
    return /* @__PURE__ */ new Set();
  }
  if (state.ownerId === author) {
    permissions.add("administrator");
    return permissions;
  }
  for (const roleId of member?.roles ?? []) {
    permissions.add(normalizePermission(roleId));
    const role = state.roles.get(roleId);
    for (const permission2 of role?.permissions ?? []) {
      permissions.add(normalizePermission(permission2));
    }
  }
  return permissions;
}
__name(channelBasePermissions, "channelBasePermissions");
function normalizedOverwritePermissions(value) {
  if (Array.isArray(value)) {
    return value.filter(
      (entry) => typeof entry === "string" && entry.trim().length > 0
    ).map(normalizePermission);
  }
  if (value instanceof Set) {
    return [...value].filter(
      (entry) => typeof entry === "string" && entry.trim().length > 0
    ).map(normalizePermission);
  }
  return [];
}
__name(normalizedOverwritePermissions, "normalizedOverwritePermissions");
function applyOverwrite(permissions, deny, allow) {
  for (const permission2 of normalizedOverwritePermissions(deny)) {
    permissions.delete(permission2);
  }
  for (const permission2 of normalizedOverwritePermissions(allow)) {
    permissions.add(permission2);
  }
}
__name(applyOverwrite, "applyOverwrite");
function everyoneOverwriteIds(state) {
  const ids = /* @__PURE__ */ new Set([state.guildId, "@everyone", "everyone"]);
  for (const [roleId, role] of state.roles) {
    if (role.name === "@everyone" || roleId === "@everyone" || roleId === "everyone") {
      ids.add(roleId);
    }
  }
  return ids;
}
__name(everyoneOverwriteIds, "everyoneOverwriteIds");
function channelPermissionOverwrites(channel2) {
  return Array.isArray(channel2.permissionOverwrites) ? channel2.permissionOverwrites : [];
}
__name(channelPermissionOverwrites, "channelPermissionOverwrites");
function channelPermissionsForMember(state, channel2, author) {
  const permissions = channelBasePermissions(state, author);
  if (permissions.has("administrator")) {
    return permissions;
  }
  const overwrites = channelPermissionOverwrites(channel2);
  const everyoneIds = everyoneOverwriteIds(state);
  const everyoneOverwrite = overwrites.find(
    (overwrite) => overwrite.kind === "role" && everyoneIds.has(overwrite.id)
  );
  if (everyoneOverwrite) {
    applyOverwrite(
      permissions,
      everyoneOverwrite.deny,
      everyoneOverwrite.allow
    );
  }
  const member = state.members.get(author);
  if (member) {
    const roleDeny = /* @__PURE__ */ new Set();
    const roleAllow = /* @__PURE__ */ new Set();
    for (const overwrite of overwrites) {
      if (overwrite.kind !== "role" || everyoneIds.has(overwrite.id) || !member.roles.has(overwrite.id)) {
        continue;
      }
      for (const permission2 of normalizedOverwritePermissions(overwrite.deny)) {
        roleDeny.add(permission2);
      }
      for (const permission2 of normalizedOverwritePermissions(
        overwrite.allow
      )) {
        roleAllow.add(permission2);
      }
    }
    applyOverwrite(permissions, roleDeny, roleAllow);
    const memberOverwrite = overwrites.find(
      (overwrite) => overwrite.kind === "member" && overwrite.id === author
    );
    if (memberOverwrite) {
      applyOverwrite(permissions, memberOverwrite.deny, memberOverwrite.allow);
    }
    const timedOutUntil = member.timedOutUntil;
    if (typeof timedOutUntil === "string" && Date.parse(timedOutUntil) > Date.now()) {
      permissions.delete("sendmessages");
      permissions.delete("speak");
    }
  }
  return permissions;
}
__name(channelPermissionsForMember, "channelPermissionsForMember");
function hasAnyPermission(permissions, names) {
  return names.some((name) => permissions.has(normalizePermission(name)));
}
__name(hasAnyPermission, "hasAnyPermission");
function canReadGuild(state, author) {
  if (author && state.bans.has(author)) {
    return false;
  }
  if (state.access === "private") {
    return Boolean(author && state.members.has(author));
  }
  return true;
}
__name(canReadGuild, "canReadGuild");
function canUseChannelPermission(state, author, channelId, permission2) {
  const channel2 = state.channels.get(channelId);
  if (!channel2 || !canReadGuild(state, author)) {
    return false;
  }
  const permissions = channelPermissionsForMember(state, channel2, author ?? "");
  return hasAnyPermission(permissions, [permission2]);
}
__name(canUseChannelPermission, "canUseChannelPermission");
function canViewChannel(state, author, channelId) {
  return canUseChannelPermission(state, author, channelId, "viewChannels");
}
__name(canViewChannel, "canViewChannel");
function assertChannelPermission(state, author, channelId, permission2, action) {
  const channel2 = assertChannelExists(state, channelId);
  const permissions = channelPermissionsForMember(state, channel2, author);
  if (!hasAnyPermission(permissions, [permission2])) {
    throw new Error(
      `User ${author} does not have ${permission2} permission to ${action}`
    );
  }
}
__name(assertChannelPermission, "assertChannelPermission");
function canManageMessagesInChannel(state, author, channelId) {
  const channel2 = state.channels.get(channelId);
  return Boolean(
    channel2 && hasAnyPermission(channelPermissionsForMember(state, channel2, author), [
      "manage_messages",
      "manageMessages"
    ])
  );
}
__name(canManageMessagesInChannel, "canManageMessagesInChannel");
function canModerateScope(state, author, scope) {
  if (state.ownerId === author) {
    return true;
  }
  const permissions = permissionSetForMember(state, author);
  if (hasAnyPermission(permissions, [
    "owner",
    "admin",
    "administrator",
    "manage_guild",
    "manage_server"
  ])) {
    return true;
  }
  switch (scope) {
    case "channels":
      return hasAnyPermission(permissions, [
        "manage_channels",
        "manageChannels"
      ]);
    case "roles":
      return hasAnyPermission(permissions, ["manage_roles", "manageRoles"]);
    case "messages":
      return hasAnyPermission(permissions, [
        "manage_messages",
        "manageMessages"
      ]);
    case "members":
      return hasAnyPermission(permissions, [
        "moderate_members",
        "moderateMembers",
        "kick_members",
        "ban_members"
      ]);
    case "events":
      return hasAnyPermission(permissions, ["manage_events", "manageEvents"]);
    case "invites":
      return hasAnyPermission(permissions, [
        "create_instant_invite",
        "createInvites",
        "manage_invites",
        "manageInvites"
      ]);
    case "apps":
      return hasAnyPermission(permissions, [
        "manage_apps",
        "manageApps",
        "manage_integrations",
        "manageIntegrations"
      ]);
    case "webhooks":
      return hasAnyPermission(permissions, [
        "manage_webhooks",
        "manageWebhooks"
      ]);
    case "guild":
      return false;
  }
}
__name(canModerateScope, "canModerateScope");
function assertCanModerateScope(state, author, type, scope) {
  if (!canModerateScope(state, author, scope)) {
    throw new Error(`User ${author} does not have permission for ${type}`);
  }
}
__name(assertCanModerateScope, "assertCanModerateScope");
function hasServerPermission(state, author, names) {
  if (state.ownerId === author) {
    return true;
  }
  const permissions = permissionSetForMember(state, author);
  return hasAnyPermission(permissions, [
    "admin",
    "administrator",
    "manage_guild",
    "manage_server",
    ...names
  ]);
}
__name(hasServerPermission, "hasServerPermission");
function rolePosition(role) {
  const position = role?.position;
  return typeof position === "number" && Number.isFinite(position) ? position : 0;
}
__name(rolePosition, "rolePosition");
function highestRolePositionForMember(state, userId) {
  if (state.ownerId === userId) {
    return Number.POSITIVE_INFINITY;
  }
  const member = state.members.get(userId);
  if (!member) {
    return Number.NEGATIVE_INFINITY;
  }
  let highest = Number.NEGATIVE_INFINITY;
  for (const roleId of member.roles) {
    const role = state.roles.get(roleId);
    if (role) {
      highest = Math.max(highest, rolePosition(role));
    }
  }
  return highest;
}
__name(highestRolePositionForMember, "highestRolePositionForMember");
function canManageRoleTarget(state, author, roleOrPosition) {
  if (!hasServerPermission(state, author, ["manage_roles", "manageRoles"])) {
    return false;
  }
  if (!state.members.has(author)) {
    return false;
  }
  if (state.ownerId === author) {
    return true;
  }
  if (roleOrPosition?.managed === true) {
    return false;
  }
  const targetPosition = typeof roleOrPosition === "number" ? roleOrPosition : roleOrPosition ? rolePosition(roleOrPosition) : Number.NEGATIVE_INFINITY;
  return highestRolePositionForMember(state, author) > targetPosition;
}
__name(canManageRoleTarget, "canManageRoleTarget");
function assertCanManageRoleTarget(state, author, type, roleOrPosition) {
  if (!canManageRoleTarget(state, author, roleOrPosition)) {
    throw new Error(`User ${author} does not have permission for ${type}`);
  }
}
__name(assertCanManageRoleTarget, "assertCanManageRoleTarget");
function normalizedRoleIds(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(
    (roleId) => typeof roleId === "string" && roleId.trim().length > 0
  );
}
__name(normalizedRoleIds, "normalizedRoleIds");
function canAssignMemberRolesTarget(state, author, targetId, roleIds) {
  if (!targetId || !hasServerPermission(state, author, ["manage_roles", "manageRoles"])) {
    return false;
  }
  const actor = state.members.get(author);
  if (!actor) {
    return false;
  }
  const target = state.members.get(targetId);
  if (!target && state.ownerId !== author) {
    return false;
  }
  if (state.ownerId === targetId) {
    return false;
  }
  if (state.ownerId !== author && target && highestRolePositionForMember(state, author) <= highestRolePositionForMember(state, targetId)) {
    return false;
  }
  return roleIds.every((roleId) => {
    const role = state.roles.get(roleId);
    if (!role) {
      return state.ownerId === author;
    }
    return canManageRoleTarget(state, author, role);
  });
}
__name(canAssignMemberRolesTarget, "canAssignMemberRolesTarget");
function assertCanAssignMemberRolesTarget(state, author, targetId, roleIds) {
  if (!canAssignMemberRolesTarget(state, author, targetId, roleIds)) {
    throw new Error(
      `User ${author} does not have permission to update roles for ${targetId}`
    );
  }
}
__name(assertCanAssignMemberRolesTarget, "assertCanAssignMemberRolesTarget");
function memberModerationPermissionNames(required) {
  switch (required) {
    case "kickMembers":
      return ["kick_members", "kickMembers"];
    case "banMembers":
      return ["ban_members", "banMembers"];
    case "moderateMembers":
      return ["moderate_members", "moderateMembers"];
  }
}
__name(memberModerationPermissionNames, "memberModerationPermissionNames");
function canModerateMemberTarget(state, author, targetId, required) {
  if (!targetId || author === targetId) {
    return false;
  }
  const actor = state.members.get(author);
  if (!actor) {
    return false;
  }
  if (!hasServerPermission(
    state,
    author,
    memberModerationPermissionNames(required)
  )) {
    return false;
  }
  if (state.ownerId === targetId) {
    return false;
  }
  const target = state.members.get(targetId);
  if (!target) {
    return true;
  }
  if (state.ownerId === author) {
    return true;
  }
  return highestRolePositionForMember(state, author) > highestRolePositionForMember(state, targetId);
}
__name(canModerateMemberTarget, "canModerateMemberTarget");
function assertCanModerateMemberTarget(state, author, targetId, required, action) {
  if (!canModerateMemberTarget(state, author, targetId, required)) {
    throw new Error(
      `User ${author} does not have permission to ${action} ${targetId}`
    );
  }
}
__name(assertCanModerateMemberTarget, "assertCanModerateMemberTarget");
function assertHasMemberModerationPermission(state, author, required, action) {
  if (!state.members.has(author) || !hasServerPermission(
    state,
    author,
    memberModerationPermissionNames(required)
  )) {
    throw new Error(`User ${author} does not have permission to ${action}`);
  }
}
__name(assertHasMemberModerationPermission, "assertHasMemberModerationPermission");
var MEMBER_UPDATE_METADATA_KEYS = /* @__PURE__ */ new Set([
  "type",
  "guildId",
  "userId",
  "reason",
  "encryptedGroupKey",
  "eventId",
  "clientEventId"
]);
var SELF_MEMBER_UPDATE_KEYS = /* @__PURE__ */ new Set([
  "nickname",
  "avatar",
  "banner",
  "bio",
  "external"
]);
var MODERATION_MEMBER_UPDATE_KEYS = /* @__PURE__ */ new Set([
  "timedOutUntil",
  "timeoutUntil",
  "isMuted",
  "isDeafened",
  "isPending"
]);
function memberUpdateKeys(body) {
  return Object.keys(body).filter(
    (key) => !MEMBER_UPDATE_METADATA_KEYS.has(key) && body[key] !== void 0
  );
}
__name(memberUpdateKeys, "memberUpdateKeys");
function validateMemberUpdate(state, author, body) {
  const targetId = typeof body.userId === "string" && body.userId.trim() ? body.userId : author;
  const keys = memberUpdateKeys(body);
  if (keys.length === 0) {
    assertIsMember(state, author);
    return;
  }
  if (keys.includes("roleIds") || keys.includes("roles")) {
    const roleIds = normalizedRoleIds(body.roleIds ?? body.roles);
    assertCanAssignMemberRolesTarget(state, author, targetId, roleIds);
    return;
  }
  if (author === targetId && keys.every((key) => SELF_MEMBER_UPDATE_KEYS.has(key))) {
    assertIsMember(state, author);
    return;
  }
  const required = keys.some(
    (key) => MODERATION_MEMBER_UPDATE_KEYS.has(key)
  ) ? "moderateMembers" : "moderateMembers";
  assertCanModerateMemberTarget(
    state,
    author,
    targetId,
    required,
    "update member"
  );
}
__name(validateMemberUpdate, "validateMemberUpdate");
function validateEvent(state, event) {
  const { body, author } = event;
  const bodyRecord = body;
  switch (bodyRecord.type) {
    case "GUILD_CREATE":
      throw new Error("GUILD_CREATE can only appear at seq 0");
    case "CHANNEL_CREATE": {
      assertCanModerateScope(state, author, bodyRecord.type, "channels");
      const channelBody = body;
      if (!channelBody.channelId?.trim()) {
        throw new Error("CHANNEL_CREATE requires a channelId");
      }
      if (state.channels.has(channelBody.channelId)) {
        throw new Error(`Channel ${channelBody.channelId} already exists`);
      }
      break;
    }
    case "ROLE_ASSIGN":
    case "ROLE_REVOKE":
      assertCanAssignMemberRolesTarget(
        state,
        author,
        bodyRecord.userId,
        typeof bodyRecord.roleId === "string" ? [bodyRecord.roleId] : []
      );
      break;
    case "ROLE_UPSERT": {
      const current = typeof bodyRecord.roleId === "string" ? state.roles.get(bodyRecord.roleId) : void 0;
      const target = current ?? (typeof bodyRecord.position === "number" ? bodyRecord.position : Number.NEGATIVE_INFINITY);
      assertCanManageRoleTarget(state, author, bodyRecord.type, target);
      break;
    }
    case "ROLE_DELETE":
      assertCanManageRoleTarget(
        state,
        author,
        bodyRecord.type,
        typeof bodyRecord.roleId === "string" ? state.roles.get(bodyRecord.roleId) : void 0
      );
      break;
    case "BAN_USER":
    case "BAN_ADD":
      assertCanModerateMemberTarget(
        state,
        author,
        bodyRecord.userId,
        "banMembers",
        "ban"
      );
      break;
    case "UNBAN_USER":
    case "BAN_REMOVE":
      assertHasMemberModerationPermission(
        state,
        author,
        "banMembers",
        "unban members"
      );
      break;
    case "EPHEMERAL_POLICY_UPDATE":
      assertCanModerateScope(
        state,
        author,
        bodyRecord.type,
        bodyRecord.type === "EPHEMERAL_POLICY_UPDATE" ? "channels" : "members"
      );
      break;
    case "MEMBER_KICK":
      assertCanModerateMemberTarget(
        state,
        author,
        bodyRecord.userId,
        "kickMembers",
        "kick"
      );
      break;
    case "REACTION_ADD":
    case "REACTION_REMOVE": {
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        bodyRecord.channelId,
        "viewChannels",
        "react in channel"
      );
      assertChannelPermission(
        state,
        author,
        bodyRecord.channelId,
        "sendMessages",
        "react in channel"
      );
      if (typeof bodyRecord.reaction !== "string" || !bodyRecord.reaction.trim()) {
        throw new Error(`${bodyRecord.type} requires a reaction`);
      }
      const message = state.messages.get(bodyRecord.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${bodyRecord.messageId} does not exist`);
      }
      if (message.channelId !== bodyRecord.channelId) {
        throw new Error(
          `Message ${bodyRecord.messageId} does not belong to channel ${bodyRecord.channelId}`
        );
      }
      if (bodyRecord.type === "REACTION_REMOVE" && typeof bodyRecord.userId === "string" && bodyRecord.userId !== author && !canManageMessagesInChannel(state, author, bodyRecord.channelId)) {
        throw new Error(`User ${author} cannot remove another user's reaction`);
      }
      break;
    }
    case "MESSAGE":
      const msgBody = body;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        msgBody.channelId,
        "viewChannels",
        "send into channel"
      );
      assertChannelPermission(
        state,
        author,
        msgBody.channelId,
        "sendMessages",
        "send into channel"
      );
      if (state.messages.has(msgBody.messageId || event.id)) {
        throw new Error(
          `Message ${msgBody.messageId || event.id} already exists`
        );
      }
      break;
    case "EDIT_MESSAGE": {
      const editBody = body;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        editBody.channelId,
        "viewChannels",
        "edit in channel"
      );
      const message = state.messages.get(editBody.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${editBody.messageId} does not exist`);
      }
      if (message.channelId !== editBody.channelId) {
        throw new Error(
          `Message ${editBody.messageId} does not belong to channel ${editBody.channelId}`
        );
      }
      if (message.authorId !== author) {
        throw new Error(
          `User ${author} cannot edit message ${editBody.messageId}`
        );
      }
      break;
    }
    case "DELETE_MESSAGE": {
      const deleteBody = body;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        deleteBody.channelId,
        "viewChannels",
        "delete in channel"
      );
      const message = state.messages.get(deleteBody.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${deleteBody.messageId} does not exist`);
      }
      if (message.channelId !== deleteBody.channelId) {
        throw new Error(
          `Message ${deleteBody.messageId} does not belong to channel ${deleteBody.channelId}`
        );
      }
      if (message.authorId !== author && !canManageMessagesInChannel(state, author, deleteBody.channelId)) {
        throw new Error(
          `User ${author} cannot delete message ${deleteBody.messageId}`
        );
      }
      break;
    }
    case "APP_OBJECT_UPSERT":
    case "APP_OBJECT_DELETE": {
      const appBody = body;
      assertCanParticipateInGuild(state, author);
      if (!appBody.namespace.trim() || !appBody.objectType.trim() || !appBody.objectId.trim()) {
        throw new Error(
          `${body.type} requires namespace, objectType, and objectId`
        );
      }
      const channelId = appBody.channelId || appBody.target?.channelId;
      if (channelId) {
        assertChannelPermission(
          state,
          author,
          channelId,
          "viewChannels",
          "publish app object in channel"
        );
      }
      const targetMessageId = appBody.target?.messageId;
      if (targetMessageId) {
        const message = state.messages.get(targetMessageId);
        if (!message || message.deleted) {
          throw new Error(`Target message ${targetMessageId} does not exist`);
        }
        if (channelId && message.channelId !== channelId) {
          throw new Error(
            `Target message ${targetMessageId} does not belong to channel ${channelId}`
          );
        }
      }
      break;
    }
    case "MEMBER_UPDATE":
      validateMemberUpdate(state, author, bodyRecord);
      break;
    case "CHECKPOINT": {
      const checkpoint = body;
      if (checkpoint.seq !== event.seq) {
        throw new Error("CHECKPOINT seq must match event seq");
      }
      if (checkpoint.guildId !== state.guildId) {
        throw new Error("CHECKPOINT guildId must match current state");
      }
      if (checkpoint.state.guildId !== state.guildId) {
        throw new Error("CHECKPOINT state guildId must match current state");
      }
      const currentRoot = checkpointStateRoot(serializeState(state));
      if (checkpoint.rootHash !== currentRoot) {
        throw new Error("CHECKPOINT rootHash does not match current state");
      }
      if (checkpointStateRoot(checkpoint.state) !== checkpoint.rootHash) {
        throw new Error("CHECKPOINT state does not match rootHash");
      }
      break;
    }
    default: {
      const adminScope = ADMIN_EVENT_TYPES.get(bodyRecord.type);
      if (adminScope) {
        assertCanModerateScope(state, author, bodyRecord.type, adminScope);
        break;
      }
      if (CHANNEL_PARTICIPATION_EVENT_TYPES.has(bodyRecord.type)) {
        if (typeof bodyRecord.channelId === "string" && bodyRecord.channelId) {
          assertChannelExists(state, bodyRecord.channelId);
        }
        assertCanParticipateInGuild(state, author);
      }
      break;
    }
  }
}
__name(validateEvent, "validateEvent");

// ../core/src/pubsub_wire.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();

// ../core/src/wire.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var InvalidCgpFrameError = class extends Error {
  static {
    __name(this, "InvalidCgpFrameError");
  }
  constructor(message) {
    super(message);
    this.name = "InvalidCgpFrameError";
  }
};
var sharedTextEncoder = typeof TextEncoder !== "undefined" ? new TextEncoder() : void 0;
var sharedTextDecoder = typeof TextDecoder !== "undefined" ? new TextDecoder() : void 0;
var BINARY_V1_MAGIC_0 = 67;
var BINARY_V1_MAGIC_1 = 71;
var BINARY_V1_MAGIC_2 = 80;
var BINARY_V1_VERSION = 1;
var BINARY_V2_VERSION = 2;
var BINARY_V2_OPCODE_GENERIC = 0;
var BINARY_V2_OPCODE_PUBLISH = 1;
var BINARY_V2_OPCODE_PUBLISH_BATCH = 2;
var BINARY_V2_PUBLISH_FLAG_CREATED_AT = 1 << 0;
var BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID = 1 << 1;
function bytesToUtf8(bytes) {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("utf8");
  }
  if (sharedTextDecoder) {
    return sharedTextDecoder.decode(bytes);
  }
  return String.fromCharCode(...bytes);
}
__name(bytesToUtf8, "bytesToUtf8");
function allocByteArray(size) {
  if (typeof Buffer !== "undefined") {
    return Buffer.allocUnsafe(size);
  }
  return new Uint8Array(size);
}
__name(allocByteArray, "allocByteArray");
function utf8ToBytes2(value) {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(value, "utf8");
  }
  if (sharedTextEncoder) {
    return sharedTextEncoder.encode(value);
  }
  return Uint8Array.from(value.split("").map((char) => char.charCodeAt(0)));
}
__name(utf8ToBytes2, "utf8ToBytes");
function encodeCgpWireFrame(frame, wireFormat) {
  if (wireFormat === "binary-v1" || wireFormat === "binary-v2") {
    const { kind, payload } = parseCgpFrame(frame);
    return wireFormat === "binary-v2" ? encodeBinaryV2Frame(kind, payload) : encodeBinaryV1Frame(kind, payload);
  }
  return wireFormat === "binary-json" ? utf8ToBytes2(frame) : frame;
}
__name(encodeCgpWireFrame, "encodeCgpWireFrame");
function stringifyCgpFrame(kind, payload) {
  return JSON.stringify([kind, payload]);
}
__name(stringifyCgpFrame, "stringifyCgpFrame");
function parseCgpFrame(raw) {
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new InvalidCgpFrameError("Frame must be valid JSON");
  }
  if (!Array.isArray(parsed) || typeof parsed[0] !== "string" || parsed[0].trim().length === 0) {
    throw new InvalidCgpFrameError("Frame must be a JSON array whose first item is a frame kind");
  }
  return { kind: parsed[0], payload: parsed[1] };
}
__name(parseCgpFrame, "parseCgpFrame");
function normalizeFrameKind(kind) {
  const normalized = kind.trim();
  if (!normalized) {
    throw new InvalidCgpFrameError("Frame kind cannot be empty");
  }
  return normalized;
}
__name(normalizeFrameKind, "normalizeFrameKind");
function isBinaryV1Frame(bytes) {
  return bytes.byteLength >= 5 && bytes[0] === BINARY_V1_MAGIC_0 && bytes[1] === BINARY_V1_MAGIC_1 && bytes[2] === BINARY_V1_MAGIC_2 && bytes[3] === BINARY_V1_VERSION;
}
__name(isBinaryV1Frame, "isBinaryV1Frame");
function isBinaryV2Frame(bytes) {
  return bytes.byteLength >= 6 && bytes[0] === BINARY_V1_MAGIC_0 && bytes[1] === BINARY_V1_MAGIC_1 && bytes[2] === BINARY_V1_MAGIC_2 && bytes[3] === BINARY_V2_VERSION;
}
__name(isBinaryV2Frame, "isBinaryV2Frame");
function writeUint16(target, offset, value) {
  target[offset] = value >>> 8 & 255;
  target[offset + 1] = value & 255;
}
__name(writeUint16, "writeUint16");
function writeUint32(target, offset, value) {
  target[offset] = value >>> 24 & 255;
  target[offset + 1] = value >>> 16 & 255;
  target[offset + 2] = value >>> 8 & 255;
  target[offset + 3] = value & 255;
}
__name(writeUint32, "writeUint32");
function readUint16(bytes, offset) {
  if (offset + 2 > bytes.byteLength) {
    throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
  }
  return bytes[offset] << 8 | bytes[offset + 1];
}
__name(readUint16, "readUint16");
function readUint32(bytes, offset) {
  if (offset + 4 > bytes.byteLength) {
    throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
  }
  return bytes[offset] << 24 >>> 0 | bytes[offset + 1] << 16 | bytes[offset + 2] << 8 | bytes[offset + 3];
}
__name(readUint32, "readUint32");
function writeFloat64(target, offset, value) {
  new DataView(target.buffer, target.byteOffset, target.byteLength).setFloat64(offset, value, false);
}
__name(writeFloat64, "writeFloat64");
function readFloat64(bytes, offset) {
  if (offset + 8 > bytes.byteLength) {
    throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
  }
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(offset, false);
}
__name(readFloat64, "readFloat64");
function encodeBinaryV1Frame(kind, payload) {
  const kindBytes = utf8ToBytes2(normalizeFrameKind(kind));
  if (kindBytes.byteLength > 255) {
    throw new InvalidCgpFrameError("Frame kind is too long for binary-v1");
  }
  const payloadBytes = utf8ToBytes2(JSON.stringify(payload ?? null));
  const frame = allocByteArray(5 + kindBytes.byteLength + payloadBytes.byteLength);
  frame[0] = BINARY_V1_MAGIC_0;
  frame[1] = BINARY_V1_MAGIC_1;
  frame[2] = BINARY_V1_MAGIC_2;
  frame[3] = BINARY_V1_VERSION;
  frame[4] = kindBytes.byteLength;
  frame.set(kindBytes, 5);
  frame.set(payloadBytes, 5 + kindBytes.byteLength);
  return frame;
}
__name(encodeBinaryV1Frame, "encodeBinaryV1Frame");
function recordPayload(payload) {
  return payload && typeof payload === "object" && !Array.isArray(payload) ? payload : void 0;
}
__name(recordPayload, "recordPayload");
function compactPublishParts(record) {
  if (record.body === void 0 || typeof record.author !== "string" || typeof record.signature !== "string") {
    return void 0;
  }
  const bodyBytes = utf8ToBytes2(JSON.stringify(record.body ?? null));
  const authorBytes = utf8ToBytes2(record.author);
  const signatureBytes = utf8ToBytes2(record.signature);
  const clientEventId = typeof record.clientEventId === "string" ? record.clientEventId : void 0;
  const clientEventIdBytes = clientEventId !== void 0 ? utf8ToBytes2(clientEventId) : void 0;
  const createdAt = Number(record.createdAt);
  const hasCreatedAt = Number.isFinite(createdAt);
  if (authorBytes.byteLength > 65535 || signatureBytes.byteLength > 65535 || (clientEventIdBytes?.byteLength ?? 0) > 65535) {
    return void 0;
  }
  let flags = 0;
  if (hasCreatedAt) flags |= BINARY_V2_PUBLISH_FLAG_CREATED_AT;
  if (clientEventIdBytes) flags |= BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID;
  const byteLength = 1 + 2 + 2 + 2 + 4 + (hasCreatedAt ? 8 : 0) + authorBytes.byteLength + signatureBytes.byteLength + (clientEventIdBytes?.byteLength ?? 0) + bodyBytes.byteLength;
  return {
    flags,
    ...hasCreatedAt ? { createdAt } : {},
    authorBytes,
    signatureBytes,
    ...clientEventIdBytes ? { clientEventIdBytes } : {},
    bodyBytes,
    byteLength
  };
}
__name(compactPublishParts, "compactPublishParts");
function writeCompactPublishPayload(target, start, parts) {
  let offset = start;
  target[offset++] = parts.flags;
  writeUint16(target, offset, parts.authorBytes.byteLength);
  offset += 2;
  writeUint16(target, offset, parts.signatureBytes.byteLength);
  offset += 2;
  writeUint16(target, offset, parts.clientEventIdBytes?.byteLength ?? 0);
  offset += 2;
  writeUint32(target, offset, parts.bodyBytes.byteLength);
  offset += 4;
  if (parts.createdAt !== void 0) {
    writeFloat64(target, offset, parts.createdAt);
    offset += 8;
  }
  target.set(parts.authorBytes, offset);
  offset += parts.authorBytes.byteLength;
  target.set(parts.signatureBytes, offset);
  offset += parts.signatureBytes.byteLength;
  if (parts.clientEventIdBytes) {
    target.set(parts.clientEventIdBytes, offset);
    offset += parts.clientEventIdBytes.byteLength;
  }
  target.set(parts.bodyBytes, offset);
  return offset + parts.bodyBytes.byteLength;
}
__name(writeCompactPublishPayload, "writeCompactPublishPayload");
function encodeCompactPublishPayload(record) {
  const parts = compactPublishParts(record);
  if (!parts) return void 0;
  const frame = allocByteArray(parts.byteLength);
  writeCompactPublishPayload(frame, 0, parts);
  return frame;
}
__name(encodeCompactPublishPayload, "encodeCompactPublishPayload");
function parseJsonBytes(bytes, message) {
  try {
    return bytes.byteLength > 0 ? JSON.parse(bytesToUtf8(bytes)) : null;
  } catch {
    throw new InvalidCgpFrameError(message);
  }
}
__name(parseJsonBytes, "parseJsonBytes");
function parseCompactPublishPayload(bytes, offset = 0) {
  if (offset + 11 > bytes.byteLength) {
    throw new InvalidCgpFrameError("Binary-v2 publish payload is truncated");
  }
  const flags = bytes[offset++];
  const authorLength = readUint16(bytes, offset);
  offset += 2;
  const signatureLength = readUint16(bytes, offset);
  offset += 2;
  const clientEventIdLength = readUint16(bytes, offset);
  offset += 2;
  const bodyLength = readUint32(bytes, offset);
  offset += 4;
  let createdAt;
  if ((flags & BINARY_V2_PUBLISH_FLAG_CREATED_AT) !== 0) {
    createdAt = readFloat64(bytes, offset);
    offset += 8;
  }
  const authorEnd = offset + authorLength;
  const signatureEnd = authorEnd + signatureLength;
  const clientEventIdEnd = signatureEnd + clientEventIdLength;
  const bodyEnd = clientEventIdEnd + bodyLength;
  if (bodyEnd > bytes.byteLength) {
    throw new InvalidCgpFrameError("Binary-v2 publish payload is truncated");
  }
  const author = bytesToUtf8(bytes.subarray(offset, authorEnd));
  const signature = bytesToUtf8(bytes.subarray(authorEnd, signatureEnd));
  const clientEventId = (flags & BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID) !== 0 ? bytesToUtf8(bytes.subarray(signatureEnd, clientEventIdEnd)) : void 0;
  const body = parseJsonBytes(bytes.subarray(clientEventIdEnd, bodyEnd), "Binary-v2 publish body must be valid JSON");
  return {
    nextOffset: bodyEnd,
    payload: {
      body,
      author,
      signature,
      ...createdAt !== void 0 ? { createdAt } : {},
      ...clientEventId !== void 0 ? { clientEventId } : {}
    }
  };
}
__name(parseCompactPublishPayload, "parseCompactPublishPayload");
function encodeBinaryV2GenericFrame(kind, payload) {
  const kindBytes = utf8ToBytes2(normalizeFrameKind(kind));
  if (kindBytes.byteLength > 255) {
    throw new InvalidCgpFrameError("Frame kind is too long for binary-v2");
  }
  const payloadBytes = utf8ToBytes2(JSON.stringify(payload ?? null));
  const frame = allocByteArray(6 + kindBytes.byteLength + payloadBytes.byteLength);
  frame[0] = BINARY_V1_MAGIC_0;
  frame[1] = BINARY_V1_MAGIC_1;
  frame[2] = BINARY_V1_MAGIC_2;
  frame[3] = BINARY_V2_VERSION;
  frame[4] = BINARY_V2_OPCODE_GENERIC;
  frame[5] = kindBytes.byteLength;
  frame.set(kindBytes, 6);
  frame.set(payloadBytes, 6 + kindBytes.byteLength);
  return frame;
}
__name(encodeBinaryV2GenericFrame, "encodeBinaryV2GenericFrame");
function encodeBinaryV2Frame(kind, payload) {
  const normalizedKind = normalizeFrameKind(kind);
  const record = recordPayload(payload);
  if (normalizedKind === "PUBLISH" && record) {
    const compact = encodeCompactPublishPayload(record);
    if (compact) {
      const frame = allocByteArray(6 + compact.byteLength);
      frame[0] = BINARY_V1_MAGIC_0;
      frame[1] = BINARY_V1_MAGIC_1;
      frame[2] = BINARY_V1_MAGIC_2;
      frame[3] = BINARY_V2_VERSION;
      frame[4] = BINARY_V2_OPCODE_PUBLISH;
      frame[5] = 0;
      frame.set(compact, 6);
      return frame;
    }
  }
  if (normalizedKind === "PUBLISH_BATCH" && record && Array.isArray(record.events)) {
    const events = [];
    for (const event of record.events) {
      const eventRecord = recordPayload(event);
      const parts = eventRecord ? compactPublishParts(eventRecord) : void 0;
      if (!parts) {
        return encodeBinaryV2GenericFrame(normalizedKind, payload);
      }
      events.push(parts);
    }
    const batchId = typeof record.batchId === "string" ? record.batchId : "";
    const batchIdBytes = utf8ToBytes2(batchId);
    if (batchIdBytes.byteLength > 65535) {
      return encodeBinaryV2GenericFrame(normalizedKind, payload);
    }
    const eventsBytes = events.reduce((sum, event) => sum + 4 + event.byteLength, 0);
    const frame = allocByteArray(12 + batchIdBytes.byteLength + eventsBytes);
    frame[0] = BINARY_V1_MAGIC_0;
    frame[1] = BINARY_V1_MAGIC_1;
    frame[2] = BINARY_V1_MAGIC_2;
    frame[3] = BINARY_V2_VERSION;
    frame[4] = BINARY_V2_OPCODE_PUBLISH_BATCH;
    frame[5] = 0;
    writeUint16(frame, 6, batchIdBytes.byteLength);
    writeUint32(frame, 8, events.length);
    let offset = 12;
    frame.set(batchIdBytes, offset);
    offset += batchIdBytes.byteLength;
    for (const event of events) {
      writeUint32(frame, offset, event.byteLength);
      offset += 4;
      offset = writeCompactPublishPayload(frame, offset, event);
    }
    return frame;
  }
  return encodeBinaryV2GenericFrame(normalizedKind, payload);
}
__name(encodeBinaryV2Frame, "encodeBinaryV2Frame");
function parseBinaryV1Frame(bytes, includeRawFrame = false) {
  if (!isBinaryV1Frame(bytes)) {
    throw new InvalidCgpFrameError("Frame is not binary-v1");
  }
  const kindLength = bytes[4] ?? 0;
  const kindStart = 5;
  const kindEnd = kindStart + kindLength;
  if (kindLength <= 0 || kindEnd > bytes.byteLength) {
    throw new InvalidCgpFrameError("Invalid binary-v1 frame kind");
  }
  const kind = normalizeFrameKind(bytesToUtf8(bytes.subarray(kindStart, kindEnd)));
  const payloadBytes = bytes.subarray(kindEnd);
  let payload = null;
  try {
    payload = payloadBytes.byteLength > 0 ? JSON.parse(bytesToUtf8(payloadBytes)) : null;
  } catch {
    throw new InvalidCgpFrameError("Binary-v1 payload must be valid JSON");
  }
  const frame = { kind, payload };
  if (includeRawFrame) {
    frame.rawFrame = stringifyCgpFrame(kind, payload);
  }
  return frame;
}
__name(parseBinaryV1Frame, "parseBinaryV1Frame");
function parseBinaryV2Frame(bytes, includeRawFrame = false) {
  if (!isBinaryV2Frame(bytes)) {
    throw new InvalidCgpFrameError("Frame is not binary-v2");
  }
  const opcode = bytes[4] ?? 0;
  if (opcode === BINARY_V2_OPCODE_PUBLISH) {
    const parsed = parseCompactPublishPayload(bytes, 6);
    if (parsed.nextOffset !== bytes.byteLength) {
      throw new InvalidCgpFrameError("Binary-v2 publish frame has trailing bytes");
    }
    const frame2 = { kind: "PUBLISH", payload: parsed.payload };
    if (includeRawFrame) {
      frame2.rawFrame = stringifyCgpFrame("PUBLISH", parsed.payload);
    }
    return frame2;
  }
  if (opcode === BINARY_V2_OPCODE_PUBLISH_BATCH) {
    if (bytes.byteLength < 12) {
      throw new InvalidCgpFrameError("Binary-v2 publish batch frame is truncated");
    }
    const batchIdLength = readUint16(bytes, 6);
    const eventCount = readUint32(bytes, 8);
    let offset = 12;
    const batchIdEnd = offset + batchIdLength;
    if (batchIdEnd > bytes.byteLength) {
      throw new InvalidCgpFrameError("Binary-v2 publish batch frame is truncated");
    }
    const batchId = bytesToUtf8(bytes.subarray(offset, batchIdEnd));
    offset = batchIdEnd;
    const events = [];
    for (let index = 0; index < eventCount; index += 1) {
      const eventLength = readUint32(bytes, offset);
      offset += 4;
      const eventEnd = offset + eventLength;
      if (eventEnd > bytes.byteLength) {
        throw new InvalidCgpFrameError("Binary-v2 publish batch event is truncated");
      }
      const parsed = parseCompactPublishPayload(bytes.subarray(offset, eventEnd));
      if (parsed.nextOffset !== eventLength) {
        throw new InvalidCgpFrameError("Binary-v2 publish batch event has trailing bytes");
      }
      events.push(parsed.payload);
      offset = eventEnd;
    }
    if (offset !== bytes.byteLength) {
      throw new InvalidCgpFrameError("Binary-v2 publish batch frame has trailing bytes");
    }
    const payload2 = {
      ...batchId ? { batchId } : {},
      events
    };
    const frame2 = { kind: "PUBLISH_BATCH", payload: payload2 };
    if (includeRawFrame) {
      frame2.rawFrame = stringifyCgpFrame("PUBLISH_BATCH", payload2);
    }
    return frame2;
  }
  if (opcode !== BINARY_V2_OPCODE_GENERIC) {
    throw new InvalidCgpFrameError(`Unsupported binary-v2 opcode: ${opcode}`);
  }
  const kindLength = bytes[5] ?? 0;
  const kindStart = 6;
  const kindEnd = kindStart + kindLength;
  if (kindLength <= 0 || kindEnd > bytes.byteLength) {
    throw new InvalidCgpFrameError("Invalid binary-v2 frame kind");
  }
  const kind = normalizeFrameKind(bytesToUtf8(bytes.subarray(kindStart, kindEnd)));
  const payload = parseJsonBytes(bytes.subarray(kindEnd), "Binary-v2 payload must be valid JSON");
  const frame = { kind, payload };
  if (includeRawFrame) {
    frame.rawFrame = stringifyCgpFrame(kind, payload);
  }
  return frame;
}
__name(parseBinaryV2Frame, "parseBinaryV2Frame");
function encodeCgpFrame(kind, payload, wireFormat) {
  if (wireFormat === "binary-v1" || wireFormat === "binary-v2") {
    return wireFormat === "binary-v2" ? encodeBinaryV2Frame(kind, payload) : encodeBinaryV1Frame(kind, payload);
  }
  return encodeCgpWireFrame(stringifyCgpFrame(kind, payload), wireFormat);
}
__name(encodeCgpFrame, "encodeCgpFrame");
function flattenByteChunks(chunks) {
  if (chunks.length === 1) {
    return chunks[0];
  }
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}
__name(flattenByteChunks, "flattenByteChunks");
function toByteChunk(value) {
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return void 0;
}
__name(toByteChunk, "toByteChunk");
function parseCgpWireData(data, options = {}) {
  const includeRawFrame = options.includeRawFrame === true;
  if (typeof data === "string") {
    const parsed2 = parseCgpFrame(data);
    return includeRawFrame ? { ...parsed2, rawFrame: data } : parsed2;
  }
  const single = toByteChunk(data);
  if (single) {
    if (isBinaryV1Frame(single)) {
      return parseBinaryV1Frame(single, includeRawFrame);
    }
    if (isBinaryV2Frame(single)) {
      return parseBinaryV2Frame(single, includeRawFrame);
    }
    const rawFrame2 = bytesToUtf8(single);
    const parsed2 = parseCgpFrame(rawFrame2);
    return includeRawFrame ? { ...parsed2, rawFrame: rawFrame2 } : parsed2;
  }
  if (Array.isArray(data)) {
    const chunks = [];
    for (const entry of data) {
      const chunk = toByteChunk(entry);
      if (chunk) chunks.push(chunk);
    }
    if (chunks.length > 0) {
      const bytes = flattenByteChunks(chunks);
      if (isBinaryV1Frame(bytes)) {
        return parseBinaryV1Frame(bytes, includeRawFrame);
      }
      if (isBinaryV2Frame(bytes)) {
        return parseBinaryV2Frame(bytes, includeRawFrame);
      }
      const rawFrame2 = bytesToUtf8(bytes);
      const parsed2 = parseCgpFrame(rawFrame2);
      return includeRawFrame ? { ...parsed2, rawFrame: rawFrame2 } : parsed2;
    }
  }
  const rawFrame = String(data ?? "");
  const parsed = parseCgpFrame(rawFrame);
  return includeRawFrame ? { ...parsed, rawFrame } : parsed;
}
__name(parseCgpWireData, "parseCgpWireData");

// ../core/src/pubsub_wire.ts
var PUBSUB_FLAG_EVENT = 1 << 0;
var PUBSUB_FLAG_EVENTS = 1 << 1;
var PUBSUB_FLAG_HEAD = 1 << 2;
var PUBSUB_FLAG_FRAME = 1 << 3;
var PUBSUB_FLAG_ID = 1 << 4;
var PUBSUB_FLAG_TOKEN = 1 << 5;
var PUBSUB_FLAG_LIVE_TOPICS_FALSE = 1 << 6;
var sharedTextEncoder2 = typeof TextEncoder !== "undefined" ? new TextEncoder() : void 0;
var sharedTextDecoder2 = typeof TextDecoder !== "undefined" ? new TextDecoder() : void 0;

// src/store.ts
init_virtual_unenv_global_polyfill_cloudflare_unenv_preset_node_process();
init_performance2();
var DEFAULT_HISTORY_LIMIT = 100;
var MAX_HISTORY_LIMIT = 500;
var DEFAULT_RANGE_LIMIT = 5e3;
var MAX_RANGE_LIMIT = 1e4;
function normalizeLimit(value, fallback, max) {
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, parsed);
}
__name(normalizeLimit, "normalizeLimit");
function normalizeSeq(value) {
  const parsed = Math.floor(Number(value));
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : void 0;
}
__name(normalizeSeq, "normalizeSeq");
function eventChannelId(event) {
  const body = event.body;
  return typeof body.channelId === "string" ? body.channelId : "";
}
__name(eventChannelId, "eventChannelId");
function eventTargetChannelId(event) {
  const body = event.body;
  const target = body.target && typeof body.target === "object" ? body.target : void 0;
  return typeof target?.channelId === "string" ? target.channelId : "";
}
__name(eventTargetChannelId, "eventTargetChannelId");
function eventChannelIds(event) {
  return [eventChannelId(event), eventTargetChannelId(event)].filter(Boolean);
}
__name(eventChannelIds, "eventChannelIds");
function shouldIncludeStructuralEvent(event, query) {
  if (!query.includeStructural) {
    return false;
  }
  if (event.body.type === "GUILD_CREATE") {
    return true;
  }
  if (event.body.type !== "CHANNEL_CREATE") {
    return false;
  }
  return !query.channelId || eventChannelId(event) === query.channelId;
}
__name(shouldIncludeStructuralEvent, "shouldIncludeStructuralEvent");
function matchesHistoryQuery(event, query) {
  if (query.beforeSeq !== void 0 && event.seq >= query.beforeSeq) {
    return false;
  }
  if (query.afterSeq !== void 0 && event.seq <= query.afterSeq) {
    return false;
  }
  if (!query.channelId) {
    return true;
  }
  if (eventChannelId(event) === query.channelId || eventTargetChannelId(event) === query.channelId) {
    return true;
  }
  return shouldIncludeStructuralEvent(event, query);
}
__name(matchesHistoryQuery, "matchesHistoryQuery");
function selectHistoryEvents(log, query) {
  const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
  const selected = [];
  if (query.afterSeq !== void 0) {
    for (const event of log) {
      if (!matchesHistoryQuery(event, query)) {
        continue;
      }
      selected.push(event);
      if (selected.length >= limit) {
        break;
      }
    }
    return selected;
  }
  for (let index = log.length - 1; index >= 0; index -= 1) {
    const event = log[index];
    if (!event || !matchesHistoryQuery(event, query)) {
      continue;
    }
    selected.push(event);
    if (selected.length >= limit) {
      break;
    }
  }
  selected.reverse();
  return selected;
}
__name(selectHistoryEvents, "selectHistoryEvents");
var D1RelayStore = class {
  constructor(db) {
    this.db = db;
  }
  static {
    __name(this, "D1RelayStore");
  }
  initPromise;
  async getLog(guildId) {
    await this.init();
    const result = await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC"
    ).bind(guildId).all();
    return result.results.map((row) => JSON.parse(row.event_json));
  }
  async getLastEvent(guildId) {
    await this.init();
    const row = await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq DESC LIMIT 1"
    ).bind(guildId).first();
    return row ? JSON.parse(row.event_json) : void 0;
  }
  async getHistory(query) {
    await this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
    const beforeSeq = normalizeSeq(query.beforeSeq);
    const afterSeq = normalizeSeq(query.afterSeq);
    const params = [query.guildId];
    const where = ["guild_id = ?"];
    if (beforeSeq !== void 0) {
      where.push("seq < ?");
      params.push(beforeSeq);
    }
    if (afterSeq !== void 0) {
      where.push("seq > ?");
      params.push(afterSeq);
    }
    const canUseChannelIndex = query.channelId && !query.includeStructural;
    if (canUseChannelIndex) {
      where.push("primary_channel_id = ?");
      params.push(query.channelId);
    }
    const order = afterSeq !== void 0 ? "ASC" : "DESC";
    const rowLimit = canUseChannelIndex ? limit : Math.min(5e3, Math.max(limit * 8, limit));
    params.push(rowLimit);
    const result = await this.db.prepare(
      `SELECT event_json FROM events WHERE ${where.join(" AND ")} ORDER BY seq ${order} LIMIT ?`
    ).bind(...params).all();
    const rows = result.results.map((row) => JSON.parse(row.event_json));
    const ordered = order === "DESC" ? rows.reverse() : rows;
    return canUseChannelIndex ? ordered : selectHistoryEvents(ordered, { ...query, limit });
  }
  async getLogRange(query) {
    await this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_RANGE_LIMIT, MAX_RANGE_LIMIT);
    const afterSeq = normalizeSeq(query.afterSeq);
    const result = afterSeq === void 0 ? await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC LIMIT ?"
    ).bind(query.guildId, limit).all() : await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?"
    ).bind(query.guildId, afterSeq, limit).all();
    return result.results.map((row) => JSON.parse(row.event_json));
  }
  async append(guildId, event) {
    await this.init();
    const channels = eventChannelIds(event);
    const primaryChannel = channels[0] || null;
    await this.db.prepare(
      "INSERT INTO events (guild_id, seq, event_id, event_json, primary_channel_id, created_at) VALUES (?, ?, ?, ?, ?, ?)"
    ).bind(
      guildId,
      event.seq,
      event.id,
      JSON.stringify(event),
      primaryChannel,
      event.createdAt
    ).run();
  }
  init() {
    if (!this.initPromise) {
      this.initPromise = this.db.exec(`
        CREATE TABLE IF NOT EXISTS events (
          guild_id TEXT NOT NULL,
          seq INTEGER NOT NULL,
          event_id TEXT NOT NULL,
          event_json TEXT NOT NULL,
          primary_channel_id TEXT,
          created_at INTEGER,
          PRIMARY KEY (guild_id, seq)
        );
        CREATE INDEX IF NOT EXISTS events_guild_channel_seq ON events (guild_id, primary_channel_id, seq);
      `).then(() => void 0);
    }
    return this.initPromise;
  }
};
var DurableObjectSqlRelayStore = class {
  constructor(sql) {
    this.sql = sql;
  }
  static {
    __name(this, "DurableObjectSqlRelayStore");
  }
  initComplete = false;
  async getLog(guildId) {
    this.init();
    return [...this.sql.exec(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC",
      guildId
    )].map((row) => JSON.parse(row.event_json));
  }
  async getLastEvent(guildId) {
    this.init();
    const row = [...this.sql.exec(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq DESC LIMIT 1",
      guildId
    )][0];
    return row ? JSON.parse(row.event_json) : void 0;
  }
  async getHistory(query) {
    this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
    const beforeSeq = normalizeSeq(query.beforeSeq);
    const afterSeq = normalizeSeq(query.afterSeq);
    const params = [query.guildId];
    const where = ["guild_id = ?"];
    if (beforeSeq !== void 0) {
      where.push("seq < ?");
      params.push(beforeSeq);
    }
    if (afterSeq !== void 0) {
      where.push("seq > ?");
      params.push(afterSeq);
    }
    const canUseChannelIndex = query.channelId && !query.includeStructural;
    if (canUseChannelIndex) {
      where.push("primary_channel_id = ?");
      params.push(query.channelId);
    }
    const order = afterSeq !== void 0 ? "ASC" : "DESC";
    const rowLimit = canUseChannelIndex ? limit : Math.min(5e3, Math.max(limit * 8, limit));
    params.push(rowLimit);
    const rows = [...this.sql.exec(
      `SELECT event_json FROM events WHERE ${where.join(" AND ")} ORDER BY seq ${order} LIMIT ?`,
      ...params
    )].map((row) => JSON.parse(row.event_json));
    const ordered = order === "DESC" ? rows.reverse() : rows;
    return canUseChannelIndex ? ordered : selectHistoryEvents(ordered, { ...query, limit });
  }
  async getLogRange(query) {
    this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_RANGE_LIMIT, MAX_RANGE_LIMIT);
    const afterSeq = normalizeSeq(query.afterSeq);
    const rows = afterSeq === void 0 ? [...this.sql.exec(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC LIMIT ?",
      query.guildId,
      limit
    )] : [...this.sql.exec(
      "SELECT event_json FROM events WHERE guild_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
      query.guildId,
      afterSeq,
      limit
    )];
    return rows.map((row) => JSON.parse(row.event_json));
  }
  async append(guildId, event) {
    this.init();
    const channels = eventChannelIds(event);
    this.sql.exec(
      "INSERT INTO events (guild_id, seq, event_id, event_json, primary_channel_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      guildId,
      event.seq,
      event.id,
      JSON.stringify(event),
      channels[0] || null,
      event.createdAt
    );
  }
  init() {
    if (this.initComplete) {
      return;
    }
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        guild_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        event_id TEXT NOT NULL,
        event_json TEXT NOT NULL,
        primary_channel_id TEXT,
        created_at INTEGER,
        PRIMARY KEY (guild_id, seq)
      );
      CREATE INDEX IF NOT EXISTS events_guild_channel_seq ON events (guild_id, primary_channel_id, seq);
    `);
    this.initComplete = true;
  }
};

// src/index.ts
var SUPPORTED_WIRE_FORMATS = ["json", "binary-json", "binary-v1", "binary-v2"];
function json(data, init) {
  return new Response(JSON.stringify(data, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...init?.headers
    }
  });
}
__name(json, "json");
function positiveInteger(value, fallback, max) {
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, parsed);
}
__name(positiveInteger, "positiveInteger");
function optionalSeq(value) {
  const parsed = Math.floor(Number(value));
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : void 0;
}
__name(optionalSeq, "optionalSeq");
function objectPayload(payload) {
  return payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
}
__name(objectPayload, "objectPayload");
function wireFormatFromValue(value) {
  return SUPPORTED_WIRE_FORMATS.includes(value) ? value : void 0;
}
__name(wireFormatFromValue, "wireFormatFromValue");
function privateKeyFromHex(value) {
  const trimmed = value?.trim() ?? "";
  if (/^[a-fA-F0-9]{64}$/.test(trimmed)) {
    const bytes = new Uint8Array(32);
    for (let index = 0; index < 32; index += 1) {
      bytes[index] = Number.parseInt(trimmed.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
  }
  return generatePrivateKey();
}
__name(privateKeyFromHex, "privateKeyFromHex");
function eventChannelId2(event) {
  const body = event.body;
  return typeof body.channelId === "string" ? body.channelId : "";
}
__name(eventChannelId2, "eventChannelId");
function eventTargetChannelId2(event) {
  const body = event.body;
  const target = body.target && typeof body.target === "object" ? body.target : void 0;
  return typeof target?.channelId === "string" ? target.channelId : "";
}
__name(eventTargetChannelId2, "eventTargetChannelId");
function eventChannelIds2(event) {
  return [eventChannelId2(event), eventTargetChannelId2(event)].filter(Boolean);
}
__name(eventChannelIds2, "eventChannelIds");
function visibleToSubscription(event, rebuilt, subscription) {
  if (subscription.guildId !== event.body.guildId) {
    return false;
  }
  if (!rebuilt || !subscription.author) {
    return true;
  }
  if (!canReadGuild(rebuilt.state, subscription.author)) {
    return false;
  }
  const requestedChannels = subscription.channels;
  const channels = eventChannelIds2(event);
  if (channels.length === 0) {
    return true;
  }
  if (requestedChannels && channels.every((channelId) => !requestedChannels.includes(channelId))) {
    return false;
  }
  return channels.some((channelId) => canViewChannel(rebuilt.state, subscription.author, channelId));
}
__name(visibleToSubscription, "visibleToSubscription");
function filterVisibleEvents(events, rebuilt, author, channels) {
  if (!rebuilt || !author) {
    return events;
  }
  if (!canReadGuild(rebuilt.state, author)) {
    return [];
  }
  return events.filter((event) => visibleToSubscription(event, rebuilt, {
    subId: "",
    guildId: event.body.guildId,
    channels,
    author
  }));
}
__name(filterVisibleEvents, "filterVisibleEvents");
function responseError(code, message, extra = {}) {
  return { code, message, ...extra };
}
__name(responseError, "responseError");
function toSocketMessage(frame) {
  if (typeof frame === "string") {
    return frame;
  }
  return frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.byteLength);
}
__name(toSocketMessage, "toSocketMessage");
function safeObjectNamePart(value) {
  const text = value?.trim();
  if (!text) {
    return void 0;
  }
  return text.slice(0, 256).replace(/[^a-zA-Z0-9:._~-]/g, "_");
}
__name(safeObjectNamePart, "safeObjectNamePart");
function decodePathPart(value) {
  if (!value) {
    return void 0;
  }
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}
__name(decodePathPart, "decodePathPart");
function relayObjectName(url) {
  const pathParts = url.pathname.split("/").filter(Boolean);
  const pathScope = pathParts[0] === "relay" ? pathParts[1] : void 0;
  const pathValue = pathParts[0] === "relay" ? decodePathPart(pathParts[2]) : void 0;
  const queryGuild = url.searchParams.get("guildId");
  const queryBucket = url.searchParams.get("bucket");
  const queryScope = url.searchParams.get("scope");
  if (pathScope === "guild") {
    const guildId2 = safeObjectNamePart(pathValue);
    if (guildId2) {
      return `guild:${guildId2}`;
    }
  }
  if (pathScope === "bucket") {
    const bucket2 = safeObjectNamePart(pathValue);
    if (bucket2) {
      return `bucket:${bucket2}`;
    }
  }
  const guildId = safeObjectNamePart(queryGuild);
  if (guildId) {
    return `guild:${guildId}`;
  }
  const bucket = safeObjectNamePart(queryBucket || queryScope);
  if (bucket) {
    return `bucket:${bucket}`;
  }
  return "global-relay";
}
__name(relayObjectName, "relayObjectName");
var RelayDO = class {
  constructor(state, env2) {
    this.state = state;
    this.env = env2;
    const storage = env2.CGP_RELAY_STORAGE || "durable-object-sql";
    this.store = storage === "d1" && env2.DB ? new D1RelayStore(env2.DB) : new DurableObjectSqlRelayStore(state.storage.sql);
    this.relayPrivateKey = privateKeyFromHex(env2.CGP_RELAY_PRIVATE_KEY_HEX);
    this.relayPublicKey = getPublicKey2(this.relayPrivateKey);
    this.relayId = env2.CGP_RELAY_ID || `cf-${this.relayPublicKey.slice(0, 16)}`;
    this.relayName = env2.CGP_RELAY_NAME || "Cloudflare CGP Relay";
    this.requireSignedReads = env2.CGP_RELAY_REQUIRE_SIGNED_READS !== "0";
    this.maxSnapshotEvents = positiveInteger(env2.CGP_RELAY_MAX_SNAPSHOT_EVENTS, 5e3, 1e5);
    this.maxHistoryEvents = positiveInteger(env2.CGP_RELAY_MAX_HISTORY_EVENTS, 500, 5e3);
    this.maxLogRangeEvents = positiveInteger(env2.CGP_RELAY_MAX_LOG_RANGE_EVENTS, 5e3, 1e4);
    this.maxPublishBatchSize = positiveInteger(env2.CGP_RELAY_MAX_PUBLISH_BATCH_SIZE, 512, 2048);
  }
  static {
    __name(this, "RelayDO");
  }
  store;
  relayName;
  relayId;
  relayPrivateKey;
  relayPublicKey;
  requireSignedReads;
  maxSnapshotEvents;
  maxHistoryEvents;
  maxLogRangeEvents;
  maxPublishBatchSize;
  stateCache = /* @__PURE__ */ new Map();
  mutexes = /* @__PURE__ */ new Map();
  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.state.acceptWebSocket(server);
    server.serializeAttachment({
      subscriptions: [],
      wireFormat: "json"
    });
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(socket, message) {
    try {
      const { kind, payload } = parseCgpWireData(message, { includeRawFrame: true });
      await this.handleFrame(socket, kind, payload);
    } catch (error) {
      this.sendFrame(socket, "ERROR", responseError("INVALID_FRAME", error?.message || "Invalid frame"));
    }
  }
  async webSocketClose() {
  }
  async webSocketError(socket, error) {
    this.sendFrame(socket, "ERROR", responseError("SOCKET_ERROR", error instanceof Error ? error.message : "Socket error"));
  }
  async handleFrame(socket, kind, payload) {
    switch (kind) {
      case "HELLO":
        this.handleHello(socket, payload);
        return;
      case "SUB":
        await this.handleSubscribe(socket, payload);
        return;
      case "GET_HISTORY":
        await this.handleHistory(socket, payload);
        return;
      case "GET_LOG_RANGE":
        await this.handleLogRange(socket, payload);
        return;
      case "GET_STATE":
        await this.handleState(socket, payload);
        return;
      case "GET_HEAD":
        await this.handleRelayHead(socket, payload, false);
        return;
      case "GET_HEADS":
        await this.handleRelayHead(socket, payload, true);
        return;
      case "GET_MEMBERS":
        await this.handleMembers(socket, payload);
        return;
      case "SEARCH":
        await this.handleSearch(socket, payload);
        return;
      case "PUBLISH":
        await this.handlePublish(socket, payload);
        return;
      case "PUBLISH_TRANSIENT":
        await this.handleTransientPublish(socket, payload);
        return;
      case "PUBLISH_BATCH":
        await this.handlePublishBatch(socket, payload);
        return;
      default:
        this.sendFrame(socket, "ERROR", responseError("UNKNOWN_FRAME", `Unsupported frame ${kind}`));
    }
  }
  handleHello(socket, payload) {
    const p = objectPayload(payload);
    const requestedWireFormat = wireFormatFromValue(p.wireFormat);
    const attachment = this.attachment(socket);
    attachment.wireFormat = requestedWireFormat || attachment.wireFormat || "json";
    socket.serializeAttachment(attachment);
    this.sendFrame(socket, "HELLO_OK", {
      protocol: "cgp/0.1",
      relayName: this.relayName,
      relayId: this.relayId,
      relayPublicKey: this.relayPublicKey,
      wireFormat: attachment.wireFormat,
      supportedWireFormats: SUPPORTED_WIRE_FORMATS,
      deployment: "cloudflare-workers",
      storage: this.env.CGP_RELAY_STORAGE || "durable-object-sql",
      plugins: []
    });
  }
  async handleSubscribe(socket, payload) {
    const p = objectPayload(payload);
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `sub-${Date.now()}`;
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const channels = Array.isArray(p.channels) ? p.channels.filter((channelId) => typeof channelId === "string" && channelId.trim().length > 0) : void 0;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "SUB requires a guildId", { subId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("SUB", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to subscribe to this guild", { subId, guildId }));
      return;
    }
    const attachment = this.attachment(socket);
    const withoutExisting = attachment.subscriptions.filter((sub) => sub.subId !== subId && sub.guildId !== guildId);
    withoutExisting.push({ subId, guildId, channels, author });
    attachment.subscriptions = withoutExisting;
    socket.serializeAttachment(attachment);
    const rawEvents = await this.store.getHistory({ guildId, limit: this.maxSnapshotEvents });
    const events = filterVisibleEvents(rawEvents, rebuilt, author, channels);
    const tailEvent = rebuilt?.endEvent ?? events[events.length - 1];
    const checkpointEvent = rebuilt?.checkpointEvent ?? events.find((event) => event.body.type === "CHECKPOINT");
    this.sendFrame(socket, "SNAPSHOT", {
      subId,
      guildId,
      events,
      endSeq: tailEvent?.seq ?? -1,
      endHash: tailEvent?.id ?? null,
      oldestSeq: events.length > 0 ? events[0].seq : null,
      newestSeq: events.length > 0 ? events[events.length - 1].seq : null,
      hasMore: events.length >= this.maxSnapshotEvents,
      checkpointSeq: checkpointEvent?.seq ?? null,
      checkpointHash: checkpointEvent?.id ?? null
    });
  }
  async handleHistory(socket, payload) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `history-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_HISTORY requires a guildId", { subId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("GET_HISTORY", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const query = {
      guildId,
      channelId: typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : void 0,
      beforeSeq: optionalSeq(p.beforeSeq),
      afterSeq: optionalSeq(p.afterSeq),
      limit: Math.min(this.maxHistoryEvents + 1, positiveInteger(p.limit, 100, this.maxHistoryEvents) + 1),
      includeStructural: p.includeStructural === true
    };
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild history", { subId, guildId }));
      return;
    }
    if (rebuilt && author && query.channelId && !canViewChannel(rebuilt.state, author, query.channelId)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this channel history", { subId, guildId, channelId: query.channelId }));
      return;
    }
    const requestedLimit = Math.max(1, (query.limit ?? 101) - 1);
    const rawEvents = await this.store.getHistory(query);
    const visibleEvents = filterVisibleEvents(rawEvents, rebuilt, author, query.channelId ? [query.channelId] : void 0);
    const hasMore = visibleEvents.length > requestedLimit;
    const events = query.afterSeq !== void 0 ? visibleEvents.slice(0, requestedLimit) : visibleEvents.slice(Math.max(0, visibleEvents.length - requestedLimit));
    const tailEvent = events[events.length - 1] ?? rebuilt?.endEvent;
    this.sendFrame(socket, "SNAPSHOT", {
      subId,
      guildId,
      channelId: query.channelId,
      events,
      endSeq: tailEvent?.seq ?? -1,
      endHash: tailEvent?.id ?? null,
      oldestSeq: events.length > 0 ? events[0].seq : null,
      newestSeq: events.length > 0 ? events[events.length - 1].seq : null,
      hasMore,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null
    });
  }
  async handleLogRange(socket, payload) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `range-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_LOG_RANGE requires a guildId", { subId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("GET_LOG_RANGE", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild log", { subId, guildId }));
      return;
    }
    const limit = positiveInteger(p.limit, this.maxLogRangeEvents, this.maxLogRangeEvents);
    const rawEvents = await this.store.getLogRange({ guildId, afterSeq: optionalSeq(p.afterSeq), limit });
    const events = filterVisibleEvents(rawEvents, rebuilt, author);
    this.sendFrame(socket, "LOG_RANGE", {
      subId,
      guildId,
      events,
      afterSeq: optionalSeq(p.afterSeq) ?? null,
      endSeq: rebuilt?.endEvent.seq ?? (events[events.length - 1]?.seq ?? -1),
      endHash: rebuilt?.endEvent.id ?? (events[events.length - 1]?.id ?? null),
      hasMore: rawEvents.length >= limit,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null
    });
  }
  async handleState(socket, payload) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `state-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_STATE requires a guildId", { subId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("GET_STATE", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt) {
      this.sendFrame(socket, "ERROR", responseError("NOT_FOUND", "Guild state not found", { subId, guildId }));
      return;
    }
    if (author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to read this guild state", { subId, guildId }));
      return;
    }
    const serialized = serializeState(rebuilt.state);
    this.sendFrame(socket, "STATE", {
      subId,
      guildId,
      state: serialized,
      rootHash: hashObject(serialized),
      endSeq: rebuilt.endEvent.seq,
      endHash: rebuilt.endEvent.id,
      checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt.checkpointEvent?.id ?? null,
      stateIncludes: {
        members: "full",
        messages: "full",
        appObjects: "full"
      }
    });
  }
  async handleRelayHead(socket, payload, includeObserved) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `head-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_HEAD requires a guildId", { subId }));
      return;
    }
    try {
      this.verifyReadRequest(includeObserved ? "GET_HEADS" : "GET_HEAD", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    const head = await this.signRelayHead(guildId, rebuilt);
    if (!head) {
      this.sendFrame(socket, "ERROR", responseError("NOT_FOUND", "Guild head not found", { subId, guildId }));
      return;
    }
    if (includeObserved) {
      this.sendFrame(socket, "RELAY_HEADS", {
        subId,
        guildId,
        heads: [head],
        quorum: {
          guildId,
          validCount: 1,
          invalidCount: 0,
          conflictCount: 0,
          canonical: { seq: head.headSeq, hash: head.headHash, count: 1 },
          conflicts: []
        }
      });
    } else {
      this.sendFrame(socket, "RELAY_HEAD", { subId, guildId, head });
    }
  }
  async handleMembers(socket, payload) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `members-${Date.now()}`;
    if (!guildId) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "GET_MEMBERS requires a guildId", { subId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("GET_MEMBERS", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt || author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "MEMBERS", { subId, guildId, members: [], nextCursor: null, hasMore: false, totalApprox: 0 });
      return;
    }
    const limit = positiveInteger(p.limit, 100, 500);
    const afterUserId = typeof p.afterUserId === "string" ? p.afterUserId : void 0;
    const members = [...rebuilt.state.members.values()].sort((left, right) => left.userId.localeCompare(right.userId));
    const start = afterUserId ? members.findIndex((member) => member.userId > afterUserId) : 0;
    const pageStart = start >= 0 ? start : members.length;
    const page = members.slice(pageStart, pageStart + limit + 1);
    const visible = page.slice(0, limit);
    const hasMore = page.length > limit;
    this.sendFrame(socket, "MEMBERS", {
      subId,
      guildId,
      members: visible,
      nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
      hasMore,
      totalApprox: members.length
    });
  }
  async handleSearch(socket, payload) {
    const p = objectPayload(payload);
    const guildId = typeof p.guildId === "string" ? p.guildId : "";
    const subId = typeof p.subId === "string" && p.subId.trim() ? p.subId : `search-${Date.now()}`;
    const query = typeof p.query === "string" ? p.query.trim() : "";
    if (!guildId || !query) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "SEARCH requires a guildId and query", { subId, guildId }));
      return;
    }
    let author;
    try {
      author = this.verifyReadRequest("SEARCH", payload);
    } catch {
      this.sendFrame(socket, "ERROR", responseError("AUTH_FAILED", "Signed read request failed", { subId, guildId }));
      return;
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (!rebuilt || author && !canReadGuild(rebuilt.state, author)) {
      this.sendFrame(socket, "SEARCH_RESULTS", { subId, guildId, query, results: [], hasMore: false, oldestSeq: null, newestSeq: null });
      return;
    }
    const channelId = typeof p.channelId === "string" && p.channelId.trim() ? p.channelId : void 0;
    if (author && channelId && !canViewChannel(rebuilt.state, author, channelId)) {
      this.sendFrame(socket, "ERROR", responseError("FORBIDDEN", "You do not have permission to search this channel", { subId, guildId, channelId }));
      return;
    }
    const limit = positiveInteger(p.limit, 50, 100);
    const needle = query.toLowerCase();
    const candidates = await this.store.getHistory({
      guildId,
      channelId,
      limit: Math.min(5e3, Math.max(limit * 20, 500))
    });
    const visible = filterVisibleEvents(candidates, rebuilt, author, channelId ? [channelId] : void 0);
    const results = visible.filter((event) => event.body.type === "MESSAGE" && typeof event.body.content === "string").filter((event) => String(event.body.content).toLowerCase().includes(needle)).slice(-limit).map((event) => ({
      type: "message",
      guildId,
      channelId: eventChannelId2(event),
      messageId: event.body.messageId,
      seq: event.seq,
      event
    }));
    const seqs = results.map((result) => result.seq);
    this.sendFrame(socket, "SEARCH_RESULTS", {
      subId,
      guildId,
      channelId,
      query,
      scopes: ["messages"],
      results,
      hasMore: visible.length > results.length,
      oldestSeq: seqs.length > 0 ? Math.min(...seqs) : null,
      newestSeq: seqs.length > 0 ? Math.max(...seqs) : null,
      checkpointSeq: rebuilt.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt.checkpointEvent?.id ?? null
    });
  }
  async handlePublish(socket, payload) {
    const ack = await this.appendPublishPayload(payload);
    if (ack.ok) {
      this.sendFrame(socket, "PUB_ACK", ack.ack);
      await this.broadcastEvent(ack.event);
    } else {
      this.sendFrame(socket, "ERROR", responseError(ack.code, ack.message, { clientEventId: ack.clientEventId }));
    }
  }
  async handlePublishBatch(socket, payload) {
    const p = objectPayload(payload);
    const batchId = typeof p.batchId === "string" ? p.batchId : void 0;
    const events = Array.isArray(p.events) ? p.events.slice(0, this.maxPublishBatchSize) : [];
    if (events.length === 0) {
      this.sendFrame(socket, "ERROR", responseError("VALIDATION_FAILED", "PUBLISH_BATCH requires events", { batchId }));
      return;
    }
    const results = [];
    const appended = [];
    for (let index = 0; index < events.length; index += 1) {
      const result = await this.appendPublishPayload(events[index]);
      if (result.ok) {
        results.push({ ok: true, ...result.ack });
        appended.push(result.event);
      } else {
        results.push({
          ok: false,
          code: result.code,
          message: result.message,
          clientEventId: result.clientEventId
        });
      }
    }
    this.sendFrame(socket, "PUB_BATCH_ACK", {
      batchId,
      results,
      truncated: Array.isArray(p.events) && p.events.length > events.length
    });
    for (const event of appended) {
      await this.broadcastEvent(event);
    }
  }
  async handleTransientPublish(socket, payload) {
    const p = payload;
    const validation = await this.validatePublishPayload(p);
    if (!validation.ok) {
      this.sendFrame(socket, "ERROR", responseError(validation.code, validation.message, { clientEventId: validation.clientEventId }));
      return;
    }
    const event = {
      id: hashObject({
        transient: true,
        body: p.body,
        author: p.author,
        signature: p.signature,
        createdAt: p.createdAt,
        clientEventId: p.clientEventId
      }),
      seq: Number.NaN,
      prevHash: null,
      createdAt: p.createdAt,
      author: p.author,
      body: p.body,
      signature: p.signature,
      transient: true
    };
    this.sendFrame(socket, "PUB_TRANSIENT_ACK", {
      clientEventId: p.clientEventId,
      guildId: p.body.guildId,
      eventId: event.id,
      seq: Number.NaN
    });
    await this.broadcastEvent(event, true);
  }
  async appendPublishPayload(payload) {
    const validation = await this.validatePublishPayload(payload);
    if (!validation.ok) {
      return validation;
    }
    const guildId = payload.body.guildId;
    return this.withGuildMutex(guildId, async () => {
      const lastEvent = await this.store.getLastEvent(guildId);
      const seq = lastEvent ? lastEvent.seq + 1 : 0;
      const prevHash = lastEvent ? lastEvent.id : null;
      const event = {
        id: "",
        seq,
        prevHash,
        createdAt: payload.createdAt,
        author: payload.author,
        body: payload.body,
        signature: payload.signature
      };
      event.id = computeEventId(event);
      let rebuilt = this.stateCache.get(guildId);
      if (seq === 0) {
        if (payload.body.type !== "GUILD_CREATE") {
          return { ok: false, code: "VALIDATION_FAILED", message: "First event must be GUILD_CREATE", clientEventId: payload.clientEventId };
        }
        rebuilt = {
          state: createInitialState(event),
          endEvent: event,
          checkpointEvent: event.body.type === "CHECKPOINT" ? event : void 0
        };
      } else {
        if (!rebuilt || rebuilt.endEvent.seq !== seq - 1 || rebuilt.endEvent.id !== prevHash) {
          rebuilt = await this.rebuildGuild(guildId);
        }
        if (!rebuilt) {
          return { ok: false, code: "VALIDATION_FAILED", message: "Guild state could not be rebuilt", clientEventId: payload.clientEventId };
        }
        try {
          validateEvent(rebuilt.state, event);
        } catch (error) {
          return { ok: false, code: "VALIDATION_FAILED", message: error?.message || "Event validation failed", clientEventId: payload.clientEventId };
        }
        rebuilt = {
          state: applyEvent(rebuilt.state, event),
          endEvent: event,
          checkpointEvent: event.body.type === "CHECKPOINT" ? event : rebuilt.checkpointEvent
        };
      }
      await this.store.append(guildId, event);
      this.cacheGuild(guildId, rebuilt);
      return {
        ok: true,
        event,
        ack: {
          clientEventId: payload.clientEventId,
          guildId,
          eventId: event.id,
          seq: event.seq,
          prevHash: event.prevHash
        }
      };
    });
  }
  async validatePublishPayload(payload) {
    const body = payload?.body;
    const guildId = body && typeof body === "object" ? body.guildId : void 0;
    if (typeof guildId !== "string" || !guildId.trim()) {
      return { ok: false, code: "VALIDATION_FAILED", message: "Publish body requires a guildId", clientEventId: payload?.clientEventId };
    }
    if (body.type === "CHECKPOINT") {
      return { ok: false, code: "VALIDATION_FAILED", message: "CHECKPOINT events are relay-maintained", clientEventId: payload.clientEventId };
    }
    if (typeof payload.author !== "string" || typeof payload.signature !== "string" || typeof payload.createdAt !== "number") {
      return { ok: false, code: "VALIDATION_FAILED", message: "Publish requires author, signature, and createdAt", clientEventId: payload?.clientEventId };
    }
    if (!verifyObject(payload.author, { body, author: payload.author, createdAt: payload.createdAt }, payload.signature)) {
      return { ok: false, code: "INVALID_SIGNATURE", message: "Signature verification failed", clientEventId: payload.clientEventId };
    }
    const rebuilt = await this.rebuildGuild(guildId);
    if (rebuilt && !canReadGuild(rebuilt.state, payload.author)) {
      return { ok: false, code: "FORBIDDEN", message: "You do not have permission to publish to this guild", clientEventId: payload.clientEventId };
    }
    return { ok: true };
  }
  verifyReadRequest(kind, payload) {
    const p = objectPayload(payload);
    const { signature, ...unsignedPayload } = p;
    if (typeof p.author !== "string" || typeof p.createdAt !== "number" || typeof signature !== "string") {
      if (this.requireSignedReads) {
        throw new Error("Missing signed read fields");
      }
      return void 0;
    }
    const ok = verify2(p.author, hashObject({ kind, payload: unsignedPayload }), signature);
    if (!ok) {
      throw new Error("Invalid read signature");
    }
    return p.author;
  }
  async signRelayHead(guildId, rebuilt) {
    const endEvent = rebuilt?.endEvent ?? await this.store.getLastEvent(guildId);
    if (!endEvent) {
      return null;
    }
    const unsigned = {
      protocol: "cgp/0.1",
      relayId: this.relayId,
      relayPublicKey: this.relayPublicKey,
      guildId,
      headSeq: endEvent.seq,
      headHash: endEvent.id,
      prevHash: endEvent.prevHash,
      checkpointSeq: rebuilt?.checkpointEvent?.seq ?? null,
      checkpointHash: rebuilt?.checkpointEvent?.id ?? null,
      observedAt: Date.now()
    };
    return {
      ...unsigned,
      signature: await sign2(this.relayPrivateKey, relayHeadId(unsigned))
    };
  }
  async rebuildGuild(guildId) {
    const cached = this.stateCache.get(guildId);
    const lastEvent = await this.store.getLastEvent(guildId);
    if (!lastEvent) {
      this.stateCache.delete(guildId);
      return void 0;
    }
    if (cached?.endEvent.id === lastEvent.id && cached.endEvent.seq === lastEvent.seq) {
      return cached;
    }
    const log = await this.store.getLog(guildId);
    if (log.length === 0) {
      return void 0;
    }
    const rebuilt = rebuildStateFromEvents(log);
    const checkpointEvent = [...log].reverse().find((event) => event.body.type === "CHECKPOINT");
    const result = {
      state: rebuilt.state,
      endEvent: log[log.length - 1],
      checkpointEvent
    };
    this.cacheGuild(guildId, result);
    return result;
  }
  cacheGuild(guildId, rebuilt) {
    this.stateCache.set(guildId, rebuilt);
    if (this.stateCache.size > 512) {
      const oldest = this.stateCache.keys().next().value;
      if (oldest) {
        this.stateCache.delete(oldest);
      }
    }
  }
  async broadcastEvent(event, transient = false) {
    const guildId = event.body.guildId;
    const rebuilt = transient ? await this.rebuildGuild(guildId) : this.stateCache.get(guildId);
    const frameByWireFormat = /* @__PURE__ */ new Map();
    for (const socket of this.state.getWebSockets()) {
      if (socket.readyState !== WebSocket.OPEN) {
        continue;
      }
      const attachment = this.attachment(socket);
      if (!attachment.subscriptions.some((subscription) => visibleToSubscription(event, rebuilt, subscription))) {
        continue;
      }
      const wireFormat = attachment.wireFormat || "json";
      let frame = frameByWireFormat.get(wireFormat);
      if (!frame) {
        frame = encodeCgpFrame("EVENT", event, wireFormat);
        frameByWireFormat.set(wireFormat, frame);
      }
      socket.send(toSocketMessage(frame));
    }
  }
  async withGuildMutex(guildId, task) {
    const previous = this.mutexes.get(guildId) ?? Promise.resolve();
    const current = previous.catch(() => void 0).then(task);
    this.mutexes.set(guildId, current);
    try {
      return await current;
    } finally {
      if (this.mutexes.get(guildId) === current) {
        this.mutexes.delete(guildId);
      }
    }
  }
  attachment(socket) {
    try {
      const attachment = socket.deserializeAttachment();
      if (attachment && Array.isArray(attachment.subscriptions)) {
        return attachment;
      }
    } catch {
    }
    return { subscriptions: [], wireFormat: "json" };
  }
  sendFrame(socket, kind, payload) {
    if (socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    const wireFormat = this.attachment(socket).wireFormat || "json";
    const frame = encodeCgpFrame(kind, payload, wireFormat);
    socket.send(toSocketMessage(frame));
    return true;
  }
};
var index_default = {
  async fetch(request, env2) {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/healthz") {
      return json({
        ok: true,
        protocol: "cgp/0.1",
        relay: "cloudflare",
        websocket: "/relay",
        scopedWebSockets: {
          guild: "/relay/guild/{guildId}",
          bucket: "/relay/bucket/{bucketId}"
        }
      });
    }
    if (url.pathname === "/relay" || url.pathname.startsWith("/relay/")) {
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      const stub = env2.RELAY.getByName(relayObjectName(url));
      return stub.fetch(request);
    }
    return new Response("Not found", { status: 404 });
  }
};
export {
  RelayDO,
  index_default as default
};
/*! Bundled license information:

@noble/hashes/esm/utils.js:
  (*! noble-hashes - MIT License (c) 2022 Paul Miller (paulmillr.com) *)

@noble/secp256k1/index.js:
  (*! noble-secp256k1 - MIT License (c) 2019 Paul Miller (paulmillr.com) *)
*/
//# sourceMappingURL=index.js.map
