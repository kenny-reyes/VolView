import {
  readDicomTags,
  readImageDicomFileSeriesWorkerFunction,
} from '@itk-wasm/dicom';
import { readImage } from '@itk-wasm/image-io';
import { WorkerPool, createWebWorker, setDefaultWebWorker } from 'itk-wasm';

const DEFAULT_NUM_WORKERS = 4;

let readDicomSeriesWorkerPool: WorkerPool | null = null;
let webWorker: Worker | null = null;

export async function ensureWorker() {
  if (webWorker) return;
  webWorker = await createWebWorker(null);
  setDefaultWebWorker(webWorker);
}

export function ensureDicomSeriesWorkerPool() {
  if (readDicomSeriesWorkerPool) return;
  // copied from read-image-dicom-file-series.ts
  const numberOfWorkers =
    typeof globalThis.navigator?.hardwareConcurrency === 'number'
      ? globalThis.navigator.hardwareConcurrency
      : DEFAULT_NUM_WORKERS;
  readDicomSeriesWorkerPool = new WorkerPool(
    numberOfWorkers,
    readImageDicomFileSeriesWorkerFunction
  );
}

export function getWorker() {
  return webWorker;
}

export function getDicomSeriesWorkerPool() {
  return readDicomSeriesWorkerPool;
}

export function terminateWorkers() {
  console.log('MVET: Terminating ITK workers...')
  if (readDicomSeriesWorkerPool) {
    readDicomSeriesWorkerPool.terminateWorkers()
    readDicomSeriesWorkerPool = null
  }
  if (webWorker) {
    webWorker.terminate()
    webWorker = null
  }
}

export async function initItkWorker() {
  // ACCION 3: Medir init-worker aislado
  performance.mark('init-worker-start')
  performance.mark('init-worker-ensureWorker-start')
  await ensureWorker()
  performance.mark('init-worker-ensureWorker-end')
  performance.mark('init-worker-ensurePool-start')
  ensureDicomSeriesWorkerPool()
  performance.mark('init-worker-ensurePool-end')
  performance.mark('init-worker-preload-start')

  // preload
  try {
    await readDicomTags(new File([], 'a.dcm'));
  } catch (err) {
    // ignore
  }
  try {
    await readImage(new File([], 'a.dcm'));
  } catch (err) {
    // ignore
  }
  performance.mark('init-worker-preload-end')
  performance.mark('init-worker-end')
  try {
    performance.measure('init-worker-total', 'init-worker-start', 'init-worker-end')
    performance.measure('init-worker-ensureWorker', 'init-worker-ensureWorker-start', 'init-worker-ensureWorker-end')
    performance.measure('init-worker-ensurePool', 'init-worker-ensurePool-start', 'init-worker-ensurePool-end')
    performance.measure('init-worker-preload', 'init-worker-preload-start', 'init-worker-preload-end')
    const m = performance.getEntriesByType('measure').filter((e) => e.name.startsWith('init-worker-')).slice(-4)
    console.group('[ACCION 3] init-worker aislado')
    m.forEach((e) => console.log(`${e.name}: ${(e.duration / 1000).toFixed(2)}s`))
    console.groupEnd()
  } catch {
    // ignore
  }
}
