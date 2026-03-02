import {
  buildSegmentGroups,
  ReadOverlappingSegmentationMeta,
  readVolumeSlice,
  splitAndSort,
} from '@/src/io/dicom';
import { Chunk, waitForChunkState } from '@/src/core/streaming/chunk';
import { Image, JsonCompatible, readImage } from '@itk-wasm/image-io';
import { getWorker } from '@/src/io/itk/worker';
import { allocateImageFromChunks } from '@/src/utils/allocateImageFromChunks';
import { TypedArray } from '@kitware/vtk.js/types';
import { Tags } from '@/src/core/dicomTags';
import vtkDataArray from '@kitware/vtk.js/Common/Core/DataArray';
import { ChunkState } from '@/src/core/streaming/chunkStateMachine';
import {
  type ChunkImage,
  ThumbnailStrategy,
  ChunkStatus,
  ChunkImageEvents,
} from '@/src/core/streaming/chunkImage';
import mitt, { Emitter } from 'mitt';
import {
  BaseProgressiveImage,
  ProgressiveImageStatus,
} from '@/src/core/progressiveImage';
import { ensureError } from '@/src/utils';
import { computed } from 'vue';
import vtkITKHelper from '@kitware/vtk.js/Common/DataModel/ITKHelper';

const { fastComputeRange } = vtkDataArray;

const DATA_RANGE_KEY = 'pixel-data-range';

function getChunkId(chunk: Chunk) {
  const metadata = Object.fromEntries(chunk.metadata!);
  const SOPInstanceUID = metadata[Tags.SOPInstanceUID];
  return SOPInstanceUID;
}

// Assume itkImage type is Uint8Array
function itkImageToURI(itkImage: Image) {
  const [width, height] = itkImage.size;
  const im = new ImageData(width, height);
  const arr32 = new Uint32Array(im.data.buffer);
  const itkBuf = itkImage.data;
  if (!itkBuf) {
    return '';
  }

  for (let i = 0; i < itkBuf.length; i += 1) {
    const byte = itkBuf[i] as number;
    // ABGR order
    // eslint-disable-next-line no-bitwise
    arr32[i] = (255 << 24) | (byte << 16) | (byte << 8) | byte;
  }

  const canvas = document.createElement('canvas');
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext('2d');
  if (ctx) {
    ctx.putImageData(im, 0, 0);
    return canvas.toDataURL('image/png');
  }
  return '';
}

async function dicomSliceToImageUri(blob: Blob) {
  const file = new File([blob], 'file.dcm');
  const itkImage = await readVolumeSlice(file, true);
  return itkImageToURI(itkImage);
}

export default class DicomChunkImage
  extends BaseProgressiveImage
  implements ChunkImage
{
  protected chunks: Chunk[];
  private chunkListeners: Array<() => void>;
  private thumbnailCache: WeakMap<Chunk, Promise<unknown>>;
  private events: Emitter<ChunkImageEvents>;
  private chunkStatus: ChunkStatus[];
  private stackModeInitialIndices: Set<number> | null;

  public segBuildInfo:
    | (JsonCompatible & ReadOverlappingSegmentationMeta)
    | null;

  constructor() {
    super();

    this.status.value = 'incomplete';
    this.loaded = computed(() => {
      return !this.loading.value && this.status.value === 'complete';
    });

    this.chunks = [];
    this.chunkListeners = [];
    this.chunkStatus = [];
    this.thumbnailCache = new WeakMap();
    this.events = mitt();
    this.segBuildInfo = null;
    this.stackModeInitialIndices = null;

    this.addEventListener('loading', (loading) => {
      this.loading.value = loading;
    });

    this.addEventListener('status', (status) => {
      this.status.value = status;
    });
  }

  getModality() {
    const meta = Object.fromEntries(this.getDicomMetadata() ?? []);
    return meta[Tags.Modality]?.trim() ?? null;
  }

  getChunkStatuses(): Array<ChunkStatus> {
    return this.chunkStatus.slice();
  }

  getDicomMetadata(chunkNum = 0) {
    if (chunkNum < 0 || chunkNum >= this.chunks.length) {
      throw RangeError('chunkNum is out of bounds');
    }
    return this.chunks[chunkNum].metadata;
  }

  getChunks() {
    return this.chunks.slice();
  }

  addEventListener<T extends keyof ChunkImageEvents>(
    type: T,
    callback: (info: ChunkImageEvents[T]) => void
  ): void {
    this.events.on(type, callback);
  }

  removeEventListener<T extends keyof ChunkImageEvents>(
    type: T,
    callback: (info: ChunkImageEvents[T]) => void
  ): void {
    this.events.off(type, callback);
  }

  dispose() {
    super.dispose();
    this.unregisterChunkListeners();
    this.events.all.clear();
    this.chunks.length = 0;
    this.vtkImageData.value.delete();
    this.chunkStatus = [];
    this.thumbnailCache = new WeakMap();
  }

  /**
   * Stack-mode loading: only decode the initial visible slices (first, middle, last)
   * for instant 2D display. Call loadAllChunks() to decode the full volume for 3D.
   */
  startLoad() {
    console.time('[PERF] Total chunk loading');
    const total = this.chunks.length;
    console.log(`[PERF] startLoad (stack-mode): ${total} chunks, loading initial slices only`);

    if (total === 0) return;

    // Load first, middle, and last slices for the 3 orthogonal 2D views
    const middle = Math.floor(total / 2);
    const initialIndices = [...new Set([0, middle, total - 1])];
    this.stackModeInitialIndices = new Set(initialIndices);

    initialIndices.forEach((idx) => {
      this.chunks[idx].loadData();
    });

    this.events.emit('loading', true);
  }

  /**
   * Load ALL remaining chunks to build the full 3D volume.
   * Call this when the user activates a 3D view.
   */
  loadAllChunks() {
    const unloaded = this.chunks.filter(
      (chunk) => chunk.state === ChunkState.MetaOnly
    );
    if (unloaded.length === 0) return;

    console.log(`[PERF] loadAllChunks: loading ${unloaded.length} remaining chunks`);
    unloaded.forEach((chunk) => {
      chunk.loadData();
    });
  }

  /**
   * Load a specific chunk by index (for on-demand slice scrolling).
   */
  loadChunkByIndex(index: number) {
    if (index >= 0 && index < this.chunks.length) {
      const chunk = this.chunks[index];
      if (chunk.state === ChunkState.MetaOnly) {
        chunk.loadData();
      }
    }
  }

  stopLoad() {
    this.chunks.forEach((chunk) => {
      chunk.stopLoad();
    });
    this.events.emit('loading', false);
  }

  async addChunks(chunks: Chunk[]) {
    console.time('[PERF] addChunks total');
    this.unregisterChunkListeners();

    const existingIds = new Set(this.chunks.map((chunk) => getChunkId(chunk)));
    const newChunks = chunks.filter(
      (chunk) => !existingIds.has(getChunkId(chunk))
    );
    newChunks.forEach((chunk) => {
      this.chunks.push(chunk);
    });

    console.time('[PERF] loadMeta + splitAndSort');
    await Promise.all(chunks.map((chunk) => chunk.loadMeta()));
    const chunksByVolume = await splitAndSort(
      this.chunks,
      (chunk) => chunk.metaBlob!
    );
    console.timeEnd('[PERF] loadMeta + splitAndSort');
    const volumes = Object.values(chunksByVolume);
    if (volumes.length !== 1)
      throw new Error('Did not get just a single volume!');

    // save the newly sorted chunk order
    this.chunks = volumes[0];

    this.chunkStatus = this.chunks.map((chunk) => {
      switch (chunk.state) {
        case ChunkState.Init:
        case ChunkState.MetaLoading:
        case ChunkState.MetaOnly:
          return ChunkStatus.NotLoaded;
        case ChunkState.DataLoading:
          return ChunkStatus.Loading;
        case ChunkState.Loaded:
          return ChunkStatus.Loaded;
        default:
          throw new Error('Chunk is in an invalid state');
      }
    });
    this.onChunksUpdated();

    this.registerChunkListeners();
    this.processNewChunks(newChunks);

    if (this.getModality() !== 'SEG') {
      console.time('[PERF] reallocateImage');
      this.reallocateImage();
      console.timeEnd('[PERF] reallocateImage');
    }
    console.timeEnd('[PERF] addChunks total');
  }

  getThumbnail(strategy: ThumbnailStrategy): Promise<any> {
    if (strategy !== ThumbnailStrategy.MiddleSlice)
      throw new Error('Can only handle MiddleSlice thumbnailing strategy');

    const middle = Math.floor(this.chunks.length / 2);
    const chunk = this.chunks[middle];

    if (!this.thumbnailCache.has(chunk)) {
      // FIXME(fli): if chunk changes, the old promise is not cancelled
      this.thumbnailCache.set(
        chunk,
        waitForChunkState(chunk, ChunkState.Loaded).then((ch) => {
          if (!ch.dataBlob) throw new Error('No chunk data');
          return dicomSliceToImageUri(ch.dataBlob);
        })
      );
    }
    return this.thumbnailCache.get(chunk)!;
  }

  private processNewChunks(chunks: Chunk[]) {
    chunks
      .filter((chunk) => chunk.state === ChunkState.Loaded)
      .forEach((_, idx) => {
        this.onChunkHasData(idx);
      });
  }

  private registerChunkListeners() {
    this.chunkListeners = [
      ...this.chunks.map((chunk, index) => {
        const stopDoneData = chunk.addEventListener('doneData', () => {
          this.onChunkHasData(index);
        });

        const stopError = chunk.addEventListener('error', (err) => {
          this.onChunkErrored(index, err);
        });

        return () => {
          stopDoneData();
          stopError();
        };
      }),
    ];
  }

  private unregisterChunkListeners() {
    while (this.chunkListeners.length) {
      this.chunkListeners.pop()!();
    }
  }

  private reallocateImage() {
    this.vtkImageData.value.delete();
    this.vtkImageData.value = allocateImageFromChunks(this.chunks);

    // recalculate image data's data range, since allocateImageFromChunks doesn't know anything about it
    const scalars = this.vtkImageData.value.getPointData().getScalars();
    this.dataRangeFromChunks().forEach(([min, max], compIdx) => {
      scalars.setRange({ min, max }, compIdx);
    });
    scalars.modified(); // so image-stats will trigger update of range
  }

  private dataRangeFromChunks() {
    const outputRanges: Array<[number, number]> = [];
    this.chunks.forEach((chunk) => {
      const ranges = chunk.getUserData(DATA_RANGE_KEY) as
        | Array<[number, number]>
        | undefined;
      if (!ranges) return;
      ranges.forEach((range, idx) => {
        const curMin = outputRanges[idx]?.[0] ?? range[0];
        const curMax = outputRanges[idx]?.[1] ?? range[1];
        outputRanges[idx] = [
          Math.min(curMin, range[0]),
          Math.max(curMax, range[1]),
        ];
      });
    });

    return outputRanges;
  }

  private async onChunkHasData(chunkIndex: number) {
    if (this.getModality() === 'SEG') {
      await this.onSegChunkHasData(chunkIndex);
    } else {
      await this.onRegularChunkHasData(chunkIndex);
    }
  }

  private async onSegChunkHasData(chunkIndex: number) {
    if (this.chunks.length !== 1 || chunkIndex !== 0)
      throw new Error('cannot handle multiple seg files');

    const [chunk] = this.chunks;
    const results = await buildSegmentGroups(
      new File([chunk.dataBlob!], 'seg.dcm')
    );
    const image = vtkITKHelper.convertItkToVtkImage(results.outputImage);
    this.vtkImageData.value.delete();
    this.vtkImageData.value = image;

    this.segBuildInfo = results.metaInfo;

    this.chunkStatus[0] = ChunkStatus.Loaded;
    this.onChunksUpdated();
  }

  private async onRegularChunkHasData(chunkIndex: number) {
    const chunkStart = performance.now();
    const chunk = this.chunks[chunkIndex];
    if (!chunk.dataBlob) throw new Error('Chunk does not have data');

    const t0 = performance.now();
    const result = await readImage(
      new File([chunk.dataBlob], `file-${chunkIndex}.dcm`),
      {
        webWorker: getWorker(),
      }
    );
    const readTime = performance.now() - t0;

    if (!result.image.data) throw new Error('No data read from chunk');

    const t1 = performance.now();
    const scalars = this.vtkImageData.value.getPointData().getScalars();
    const pixelData = scalars.getData() as TypedArray;

    const dims = this.vtkImageData.value.getDimensions();
    const offset =
      dims[0] * dims[1] * scalars.getNumberOfComponents() * chunkIndex;
    pixelData.set(result.image.data as TypedArray, offset);
    const copyTime = performance.now() - t1;

    const rangeAlreadyInitialized = this.chunkStatus.some(
      (status) => status === ChunkStatus.Loaded
    );

    // update the data range
    const t2 = performance.now();
    const chunkDataRange: Array<[number, number]> = [];
    for (let comp = 0; comp < scalars.getNumberOfComponents(); comp++) {
      const { min, max } = fastComputeRange(
        result.image.data as unknown as number[],
        comp,
        scalars.getNumberOfComponents()
      );
      chunkDataRange.push([min, max]);

      const curRange = scalars.getRange(comp);

      const newMin = rangeAlreadyInitialized ? Math.min(min, curRange[0]) : min;
      const newMax = rangeAlreadyInitialized ? Math.max(max, curRange[1]) : max;
      scalars.setRange({ min: newMin, max: newMax }, comp);
    }
    scalars.modified(); // so image-stats will trigger update of range
    const rangeTime = performance.now() - t2;

    chunk.setUserData(DATA_RANGE_KEY, chunkDataRange);

    this.chunkStatus[chunkIndex] = ChunkStatus.Loaded;
    this.events.emit('chunkLoad', {
      chunk,
      updatedExtent: [0, dims[0] - 1, 0, dims[1] - 1, chunkIndex, chunkIndex],
    });
    this.onChunksUpdated();

    this.vtkImageData.value.modified();
    const totalChunkTime = performance.now() - chunkStart;

    // Log every 100th chunk to avoid flooding, plus first and last
    const loadedCount = this.chunkStatus.filter(s => s === ChunkStatus.Loaded).length;
    if (chunkIndex === 0 || chunkIndex === this.chunks.length - 1 || chunkIndex % 100 === 0) {
      console.log(`[PERF] chunk ${chunkIndex}/${this.chunks.length}: readImage=${readTime.toFixed(0)}ms copy=${copyTime.toFixed(0)}ms range=${rangeTime.toFixed(0)}ms total=${totalChunkTime.toFixed(0)}ms (${loadedCount}/${this.chunks.length} loaded)`);
    }
    if (loadedCount === this.chunks.length) {
      console.timeEnd('[PERF] Total chunk loading');
      console.log(`[PERF] ALL ${this.chunks.length} chunks loaded!`);
    }
  }

  private onChunkErrored(chunkIndex: number, err: unknown) {
    this.chunkStatus[chunkIndex] = ChunkStatus.Errored;
    this.events.emit('chunkError', {
      chunk: this.chunks[chunkIndex],
      error: err,
    });
    this.events.emit('error', ensureError(err));
    this.onChunksUpdated();
  }

  private computeStatus(): ProgressiveImageStatus {
    for (let i = 0; i < this.chunkStatus.length; i++) {
      if (this.chunkStatus[i] !== ChunkStatus.Loaded) return 'incomplete';
    }
    return 'complete';
  }

  private onChunksUpdated() {
    const status = this.computeStatus();
    this.events.emit('status', status);

    // Stack-mode: dismiss loading spinner once initial slices are ready
    if (this.stackModeInitialIndices && this.stackModeInitialIndices.size > 0) {
      const initialReady = [...this.stackModeInitialIndices].every(
        (idx) => this.chunkStatus[idx] === ChunkStatus.Loaded
      );
      if (initialReady) {
        console.log('[PERF] Stack-mode: initial slices ready — dismissing loading spinner');
        this.stackModeInitialIndices = null; // Only fire once
        this.events.emit('loading', false);
        return;
      }
    }

    if (status === 'complete') {
      this.events.emit('loading', false);
    }
  }
}
