#ifndef SRC_CONFIG_H_
#define SRC_CONFIG_H_

#define RAI_TENSORFLOW_BACKEND
//#define RAI_PYTORCH_BACKEND
//#define RAI_ONNXRUNTIME_BACKEND

typedef enum {
  RAI_BACKEND_TENSORFLOW = 0,
  RAI_BACKEND_PYTORCH,
  RAI_BACKEND_ONNXRUNTIME,
} RAI_Backend;

//#define RAI_COPY_RUN_INPUT
#define RAI_COPY_RUN_OUTPUT

#endif /* SRC_CONFIG_H_ */