const fullUrl = (relative) => {
  const origin = window.location.origin;
  console.log(`itkConfig: origin=${origin}, relative=${relative}`);
  return `${origin}${relative}`;
};

const itkConfig = {
  pipelineWorkerUrl: ('/itk/itk-wasm-pipeline.min.worker.js'),
  imageIOUrl: fullUrl('/itk/image-io'),
  meshIOUrl: fullUrl('/itk/mesh-io'),
  pipelinesUrl: fullUrl('/itk/pipelines'),
};

export default itkConfig;
