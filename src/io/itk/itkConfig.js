const fullUrl = (relative) => {
  const isFileProtocol = window.location.protocol === 'file:';

  if (isFileProtocol) {
    const relativePath = relative.replace(/^\/+/, '');
    return new URL(relativePath, document.baseURI).href;
  }

  const origin = window.location.origin;
  return `${origin}${relative}`;
};

const itkConfig = {
  pipelineWorkerUrl: fullUrl('/itk/itk-wasm-pipeline.min.worker.js'),
  imageIOUrl: fullUrl('/itk/image-io'),
  meshIOUrl: fullUrl('/itk/mesh-io'),
  pipelinesUrl: fullUrl('/itk/pipelines'),
};

export default itkConfig;
