const fullUrl = (relative) => {
  // In development, detect the current port automatically
  if (import.meta.env.DEV) {
    const currentPort = window.location.port || '3000';
    return `http://localhost:${currentPort}${relative}`;
  }

  // Production: use document location
  const u = new URL(document.location);
  const origin = u.origin;
  const pathParts = u.pathname.split('/');
  pathParts.pop();

  const url = origin + pathParts.join('/') + relative;
  return url;
};

const itkConfig = {
  pipelineWorkerUrl: fullUrl('/itk/itk-wasm-pipeline.min.worker.js'),
  imageIOUrl: fullUrl('/itk/image-io'),
  meshIOUrl: fullUrl('/itk/mesh-io'),
  pipelinesUrl: fullUrl('/itk/pipelines'),
};

// ACCION 5: Verificar puerto/servidor itk (log en dev para comparar web vs Electron)
if (import.meta.env.DEV) {
  const env = typeof navigator !== 'undefined' && navigator.userAgent?.toLowerCase().includes('electron') ? 'Electron' : 'Web'
  console.log(`[ACCION 5] itk URLs (${env}):`, {
    origin: typeof window !== 'undefined' ? window.location.origin : 'N/A',
    port: typeof window !== 'undefined' ? window.location.port : 'N/A',
    pipelineWorkerUrl: itkConfig.pipelineWorkerUrl,
    pipelinesUrl: itkConfig.pipelinesUrl
  })
}

export default itkConfig;
