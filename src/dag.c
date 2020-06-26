/**
 * dag.c
 *
 * Contains the helper methods for both parsing, running the command in the
 * background, and replying DAG structured commands.
 */

#include "dag.h"

#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "model.h"
#include "redisai.h"
#include "rmutil/alloc.h"
#include "rmutil/args.h"
#include "run_info.h"
#include "stats.h"
#include "tensor.h"
#include "util/arr_rm_alloc.h"
#include "util/dict.h"
#include "util/queue.h"


void *RedisAI_DagRunSessionStep(RedisAI_RunInfo *rinfo, const char *devicestr, int *progress) {
  RAI_DagOp *currentOp = NULL;

  if (rinfo->dagComplete) {
    *progress = 0;
    return NULL;
  }

  // DONE DAG
  // acquire mutex
  // this is not strictly needed for access, but we may have contention
  // if threads per queue are greater than one
  // To simplify things we may well do everything under a mutex
  bool all_complete = true;
  bool last_op = false;
  pthread_mutex_lock(&rinfo->dagMutex);
  for (size_t i = 0; i < array_len(rinfo->dagOps); i++) {
    if (rinfo->dagOps[i]->result >= 0) {
      continue;
    }

    if (strcasecmp(devicestr, rinfo->dagOps[i]->devicestr) != 0) {
      continue;
    }

    // if (rinfo->dagOps[i]->)

    if (rinfo->dagOps[i]->result == -1) {
      all_complete = false;

      // TODO DAG
      // 1. check that the op is compatibile with the current device [DONE]
      // 2. check that all results are realized
      //////////////// PROBLEM: we don't know which ones because they
      ////////////////          haven't been parsed yet :-(
      ////////////////          We need to defer the decision to later
      ////////////////          once we know the keys
      ////////////////          So basically we need to copy the keys
      ////////////////          of the local scope, so that we can
      ////////////////          check if they are there
      // SOLUTION: we can get the currentOp and execute parsing (see
      // below). If parsing is ok (aka localContext contains all inputs)
      // then we execute, otherwise we don't proceed
      // We only have to make sure we exit the parser managing memory
      // correctly (ie not leaking)
      currentOp = rinfo->dagOps[i];
      
      // TODO DAG
      // in theory we could get to the end while other devices
      // are running other prior computations
      // in this case we shouldn't unblock

      // TODO DAG FIX
      // Worker can be done even if not last op (if no other ops)
      // Think about this
      if (i == array_len(rinfo->dagOps) - 1) {
        last_op = true;
      }
      break;
    }
  }
  pthread_mutex_unlock(&rinfo->dagMutex);

  // TODO DAG
  // If results are unrealized so that computation cannot proceed, 
  // then return making sure progress is set to 0

  // if (currentOp == NULL && all_complete == 0) {
  if (currentOp == NULL) {
    *progress = 0;
    return NULL;
  }

  // TODO DAG
  // Decide whether to keep this here (it requires one extra round)
  // or keep it here just to be safe

  if (currentOp == NULL && all_complete == 1 &&
      rinfo->dagMaster && rinfo->client != NULL) {
    *progress = 0;
    rinfo->dagComplete = 1;
    RedisModule_UnblockClient(rinfo->client, rinfo);
    return NULL;
  }

  // If all complete (ie currentOp == NULL) and all results are realized
  // then unblock.
  // It would be best to unblock here, rather than at the end.
  // Unless we refuse to perform the last operation if not all
  // results have been realized

  // if (currentOp == NULL) {
       // TODO DAG
       // turn this into an error message
  //   assert(0);
  //   return NULL;
  // }


  // TODO DAG
  // we must make sure we are not creating race conditions on currrentOp; 
  // the only variable we access above is `result`. We need to acquire
  // a lock in order to set result (FIXME)
  // printf("%d %s\n", currentOp->commandType, devicestr);

  switch (currentOp->commandType) {
    case REDISAI_DAG_CMD_TENSORSET: {
      RAI_Tensor *t = NULL;
      const int parse_result = RAI_parseTensorSetArgs(
          NULL, currentOp->argv, currentOp->argc, &t, 0, currentOp->err);
      if (parse_result > 0) {
        const char *key_string =
            RedisModule_StringPtrLen(currentOp->argv[1], NULL);
        const char *dictKey = RedisModule_Strdup(key_string);
        pthread_mutex_lock(&rinfo->dagMutex);
        AI_dictReplace(rinfo->dagTensorsContext, (void*)dictKey, t);
        pthread_mutex_unlock(&rinfo->dagMutex);
        currentOp->result = REDISMODULE_OK;
      } else {
        currentOp->result = REDISMODULE_ERR;
        *progress = 0;
      }
      break;
    }
    case REDISAI_DAG_CMD_TENSORGET: {
      const char *key_string =
          RedisModule_StringPtrLen(currentOp->argv[1], NULL);
      RAI_Tensor *t = NULL;
      pthread_mutex_lock(&rinfo->dagMutex);
      currentOp->result = RAI_getTensorFromLocalContext(
          NULL, rinfo->dagTensorsContext, key_string, &t, currentOp->err);
      pthread_mutex_unlock(&rinfo->dagMutex);
      if (currentOp->result == REDISMODULE_OK) {
        RAI_Tensor *outTensor = NULL;
        // TODO: check tensor copy return value
        RAI_TensorDeepCopy(t, &outTensor);
        array_append(currentOp->outTensors, outTensor);
        currentOp->result = REDISMODULE_OK;
        *progress = 0;
      }
      break;
    }
    case REDISAI_DAG_CMD_MODELRUN: {
      pthread_mutex_lock(&rinfo->dagMutex);
      // const int parse_result = RedisAI_Parse_ModelRun_RedisCommand(
      //     NULL, currentOp->argv, currentOp->argc, &(currentOp->mctx),
      //     &(currentOp->outkeys), &(currentOp->mctx->model), 1,
      //     &(rinfo->dagTensorsContext), currentOp->err);
      // TODO DAG
      // get inkeys from local context
      RAI_Error error = {0};
      RAI_Tensor* inputTensors[array_len(currentOp->inkeys)];
      for (int i=0; i<array_len(currentOp->inkeys); i++) {
        RAI_Tensor *inputTensor;
        const int get_result = RAI_getTensorFromLocalContext(
            NULL, rinfo->dagTensorsContext, RedisModule_StringPtrLen(currentOp->inkeys[i], NULL), &inputTensor, &error);
        if (get_result == REDISMODULE_ERR) {
          *progress = 0;
          pthread_mutex_unlock(&rinfo->dagMutex);
          return NULL;
        }
        inputTensors[i] = inputTensor;
      }

      for (int i=0; i<array_len(currentOp->inkeys); i++) {
        const char *opname = NULL;
        if (currentOp->mctx->model->inputs) {
          opname = currentOp->mctx->model->inputs[i];
        }
        if (!RAI_ModelRunCtxAddInput(currentOp->mctx, opname, inputTensors[i])) {
          // TODO DAG : proper error here
          return NULL;
        }
      }
 
      for (int i=0; i<array_len(currentOp->outkeys); i++) {
        const char *opname = NULL;
        if (currentOp->mctx->model->inputs) {
          opname = currentOp->mctx->model->outputs[i];
        }
        if (!RAI_ModelRunCtxAddOutput(currentOp->mctx, opname)) {
          // TODO DAG : proper error here
          return NULL;
        }
      }
      pthread_mutex_unlock(&rinfo->dagMutex);

      RAI_ModelRunCtx *mctxs[1];
      mctxs[0] = currentOp->mctx;
      currentOp->result = REDISMODULE_OK;
      const long long start = ustime();
      currentOp->result = RAI_ModelRun(mctxs, 1, currentOp->err);
      currentOp->duration_us = ustime() - start;
      const size_t noutputs = RAI_ModelRunCtxNumOutputs(currentOp->mctx);
      for (size_t outputNumber = 0; outputNumber<noutputs; outputNumber++) {
        RAI_Tensor *tensor = RAI_ModelRunCtxOutputTensor(currentOp->mctx, outputNumber);
        if (tensor) {
          const char *key_string = RedisModule_StringPtrLen(
              currentOp->outkeys[outputNumber], NULL);
          const char *dictKey = RedisModule_Strdup(key_string);
          pthread_mutex_lock(&rinfo->dagMutex);
          AI_dictReplace(rinfo->dagTensorsContext, (void*)dictKey, tensor);
          pthread_mutex_unlock(&rinfo->dagMutex);
        } else {
          RAI_SetError(currentOp->err, RAI_EMODELRUN,
                       "ERR output tensor on DAG's MODELRUN was null");
          currentOp->result = REDISMODULE_ERR;
        }
      }
      break;
    }
    case REDISAI_DAG_CMD_SCRIPTRUN: {
      pthread_mutex_lock(&rinfo->dagMutex);
      // const int parse_result = RedisAI_Parse_ScriptRun_RedisCommand(
      //         NULL, currentOp->argv, currentOp->argc, &(currentOp->sctx),
      //         &(currentOp->outkeys), &(currentOp->sctx->script), 1,
      //         &(rinfo->dagTensorsContext), currentOp->err);
      // TODO DAG
      // get inkeys from local context
      RAI_Error error = {0};
      RAI_Tensor* inputTensors[array_len(currentOp->inkeys)];
      for (int i=0; i<array_len(currentOp->inkeys); i++) {
        RAI_Tensor *inputTensor;
        const int get_result = RAI_getTensorFromLocalContext(
            NULL, rinfo->dagTensorsContext, RedisModule_StringPtrLen(currentOp->inkeys[i], NULL), &inputTensor, &error);
        if (get_result == REDISMODULE_ERR) {
          *progress = 0;
          pthread_mutex_unlock(&rinfo->dagMutex);
          return NULL;
        }
        inputTensors[i] = inputTensor;
      }

      for (int i=0; i<array_len(currentOp->inkeys); i++) {
        if (!RAI_ScriptRunCtxAddInput(currentOp->sctx, inputTensors[i], &error)) {
          // TODO DAG : proper error here
          return NULL;
        }
      }

      for (int i=0; i<array_len(currentOp->outkeys); i++) {
        if (!RAI_ScriptRunCtxAddOutput(currentOp->sctx)) {
          // TODO DAG : proper error here
          return NULL;
        }
      } 
      pthread_mutex_unlock(&rinfo->dagMutex);

      currentOp->result = REDISMODULE_OK;
      const long long start = ustime();
      currentOp->result = RAI_ScriptRun(currentOp->sctx, currentOp->err);
      currentOp->duration_us = ustime() - start;
      const size_t noutputs = RAI_ScriptRunCtxNumOutputs(currentOp->sctx);
      for (size_t outputNumber = 0; outputNumber < noutputs;
         outputNumber++) {
        RAI_Tensor *tensor =
              RAI_ScriptRunCtxOutputTensor(currentOp->sctx, outputNumber);
        if (tensor) {
          const char *key_string = RedisModule_StringPtrLen(
                  currentOp->outkeys[outputNumber], NULL);
          const char *dictKey = RedisModule_Strdup(key_string);
          pthread_mutex_lock(&rinfo->dagMutex);
          AI_dictReplace(rinfo->dagTensorsContext, (void*)dictKey, tensor);
          pthread_mutex_unlock(&rinfo->dagMutex);
        } else {
          RAI_SetError(currentOp->err, RAI_EMODELRUN,
                       "ERR output tensor on DAG's SCRIPTRUN was null");
          currentOp->result = REDISMODULE_ERR;
        }
      }
      break;
    }
    default: {
      /* unsupported DAG's command */
      RAI_SetError(currentOp->err, RAI_EDAGRUN,
                   "ERR unsupported command within DAG");
      currentOp->result = REDISMODULE_ERR;
      break;
    }
  }

  if (currentOp->result == REDISMODULE_OK) {
    *progress = 1;
  }
  else {
    pthread_mutex_lock(&rinfo->dagMutex);
    *rinfo->dagError = 1;
    pthread_mutex_unlock(&rinfo->dagMutex);
    if (rinfo->dagMaster && rinfo->client != NULL) {
      RedisModule_UnblockClient(rinfo->client, rinfo);
    }
    return NULL;
  }

  if (last_op) {
    rinfo->dagComplete = 1;
  }

  if (last_op && rinfo->dagMaster && rinfo->client != NULL) {
    RedisModule_UnblockClient(rinfo->client, rinfo);
  }

  return NULL;
}

int RedisAI_DagRun_Reply(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc) {
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  RedisAI_RunInfo *rinfo = RedisModule_GetBlockedClientPrivateData(ctx);

  int dag_error = 0;
  char *detail_oneline;

  for (size_t i = 0; i < array_len(rinfo->dagOps); i++) {
    RAI_DagOp *currentOp = rinfo->dagOps[i];
    if (currentOp->result == REDISMODULE_ERR) {
      dag_error = 1;
      detail_oneline = currentOp->err->detail_oneline;
    }
  }
 
  if (dag_error) {
    // return RedisModule_ReplyWithError(ctx, "ERR Error from DAG");
    return RedisModule_ReplyWithError(ctx, detail_oneline);
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  for (size_t i = 0; i < array_len(rinfo->dagOps); i++) {
    RAI_DagOp *currentOp = rinfo->dagOps[i];
    switch (currentOp->commandType) {
      case REDISAI_DAG_CMD_TENSORSET: {
        rinfo->dagReplyLength++;
        if (currentOp->result == REDISMODULE_ERR) {
          RedisModule_ReplyWithError(ctx, currentOp->err->detail_oneline);
          dag_error = 1;
        } else {
          RedisModule_ReplyWithSimpleString(ctx, "OK");
        }
        break;
      }

      case REDISAI_DAG_CMD_TENSORGET: {
        rinfo->dagReplyLength++;
        if (currentOp->result == REDISMODULE_ERR) {
          RedisModule_ReplyWithError(ctx, currentOp->err->detail_oneline);
          dag_error = 1;
        } else {
          if (array_len(currentOp->outTensors) > 0) {
            RAI_Tensor *tensor = currentOp->outTensors[0];
            RAI_parseTensorGetArgs(ctx, currentOp->argv, currentOp->argc,
                                   tensor);
          } else {
            RedisModule_ReplyWithError(
                ctx, "ERR error getting tensor from local context");
          }
        }
        break;
      }

      case REDISAI_DAG_CMD_MODELRUN: {
        rinfo->dagReplyLength++;
        struct RedisAI_RunStats *rstats = NULL;
        const char *runkey =
            RedisModule_StringPtrLen(currentOp->runkey, NULL);
        RAI_GetRunStats(runkey,&rstats);
        if (currentOp->result == REDISMODULE_ERR) {
          RAI_SafeAddDataPoint(rstats,0,1,1,0);
          RedisModule_ReplyWithError(ctx, currentOp->err->detail_oneline);
          dag_error = 1;
        } else {
          RAI_SafeAddDataPoint(rstats,currentOp->duration_us,1,0,0);
          RedisModule_ReplyWithSimpleString(ctx, "OK");
        }
        break;
      }

        case REDISAI_DAG_CMD_SCRIPTRUN: {
          rinfo->dagReplyLength++;
          struct RedisAI_RunStats *rstats = NULL;
          const char *runkey = RedisModule_StringPtrLen(currentOp->runkey, NULL);
          RAI_GetRunStats(runkey,&rstats);
          if (currentOp->result == REDISMODULE_ERR) {
            RAI_SafeAddDataPoint(rstats,0,1,1,0);
            RedisModule_ReplyWithError(ctx, currentOp->err->detail_oneline);
            dag_error = 1;
          } else {
            RAI_SafeAddDataPoint(rstats,currentOp->duration_us,1,0,0);
            RedisModule_ReplyWithSimpleString(ctx, "OK");
          }
          break;
        }

      default:
        /* no-op */
        break;
    }
  }

  AI_dictIterator *persist_iter =
      AI_dictGetSafeIterator(rinfo->dagTensorsPersistentContext);
  AI_dictEntry *persist_entry = AI_dictNext(persist_iter);
  while (persist_entry) {
    const char *persist_key_name = AI_dictGetKey(persist_entry);
    AI_dictEntry *tensor_entry =
        AI_dictFind(rinfo->dagTensorsContext, persist_key_name);
    if (tensor_entry) {
      RAI_Tensor *tensor = AI_dictGetVal(tensor_entry);
      RedisModuleKey *key;
      RedisModuleString *tensor_keyname = RedisModule_CreateString(
          ctx, persist_key_name, strlen(persist_key_name));
      const int status = RAI_OpenKey_Tensor(
          ctx, tensor_keyname, &key, REDISMODULE_READ | REDISMODULE_WRITE);
      if (status == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "ERR could not save tensor");
        rinfo->dagReplyLength++;
      } else {
        if (RedisModule_ModuleTypeSetValue(key, RedisAI_TensorType, tensor) !=
            REDISMODULE_OK) {
          RedisModule_ReplyWithError(ctx, "ERR could not save tensor");
          rinfo->dagReplyLength++;
        }
      }
      RedisModule_CloseKey(key);
      RedisAI_ReplicateTensorSet(ctx, tensor_keyname, tensor);
    } else {
      RedisModule_ReplyWithError(
          ctx, "ERR specified persistent key that was not used on DAG");
      rinfo->dagReplyLength++;

      RedisModule_Log(ctx, "warning",
                      "on DAGRUN's PERSIST pecified persistent key (%s) that "
                      "was not used on DAG. Logging all local context keys",
                      persist_key_name);
      AI_dictIterator *local_iter =
          AI_dictGetSafeIterator(rinfo->dagTensorsContext);
      AI_dictEntry *local_entry = AI_dictNext(local_iter);
      while (local_entry) {
        const char *localcontext_key_name = AI_dictGetKey(local_entry);
        RedisModule_Log(ctx, "warning", "DAG's local context key (%s)",
                        localcontext_key_name);
        local_entry = AI_dictNext(local_iter);
      }

      for (size_t opN = 0; opN < array_len(rinfo->dagOps); opN++) {
        RedisModule_Log(
            ctx, "warning", "DAG's op n#  %d - cmdType %d ( argc %d )", opN,
            rinfo->dagOps[opN]->commandType, rinfo->dagOps[opN]->argc);
      }
    }

    persist_entry = AI_dictNext(persist_iter);
  }
  AI_dictReleaseIterator(persist_iter);
  RedisModule_ReplySetArrayLength(ctx, rinfo->dagReplyLength);
  RAI_FreeRunInfo(ctx, rinfo);
  return REDISMODULE_OK;
}

/**
 * DAGRUN Building Block to parse [LOAD <nkeys> key1 key2... ]
 */
int RAI_parseDAGLoadArgs(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc, AI_dict **loadedContextDict,
                         AI_dict **localContextDict,
                         const char *chaining_operator) {
  if (argc < 3) {
    RedisModule_WrongArity(ctx);
    return -1;
  }

  long long n_keys;
  const int retval = RedisModule_StringToLongLong(argv[1], &n_keys);
  if (retval != REDISMODULE_OK || n_keys <= 0) {
    RedisModule_ReplyWithError(
        ctx, "ERR invalid or negative value found in number of keys to LOAD");
    return -1;
  }

  int number_loaded_keys = 0;
  int separator_flag = 0;
  size_t argpos = 2;
  for (; (argpos <= argc - 1) && (number_loaded_keys < n_keys); argpos++) {
    const char *arg_string = RedisModule_StringPtrLen(argv[argpos], NULL);
    if (!strcasecmp(arg_string, chaining_operator)) {
      separator_flag = 1;
      break;
    } else {
      RAI_Tensor *t;
      RedisModuleKey *key;
      const int status = RAI_GetTensorFromKeyspace(ctx, argv[argpos], &key, &t,
                                                   REDISMODULE_READ);
      if (status == REDISMODULE_ERR) {
        RedisModule_Log(
            ctx, "warning",
            "on DAGRUN's LOAD could not load tensor %s from keyspace",
            arg_string);
        return -1;
      }
      RedisModule_CloseKey(key);
      const char *dictKey = RedisModule_Strdup(arg_string);
      AI_dictAdd(*localContextDict, (void*)dictKey, t);
      const char *keyspacePersistKey = RedisModule_Strdup(dictKey);
      AI_dictAdd(*loadedContextDict, (void*)keyspacePersistKey, (void *)1);
      number_loaded_keys++;
    }
  }
  if (number_loaded_keys != n_keys) {
    RedisModule_WrongArity(ctx);
    return -1;
  }
  return argpos;
}

/**
 * DAGRUN Building Block to parse [PERSIST <nkeys> key1 key2... ]
 */
int RAI_parseDAGPersistArgs(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc, AI_dict **persistContextDict,
                            const char *chaining_operator) {
  if (argc < 3) {
    RedisModule_WrongArity(ctx);
    return -1;
  }

  long long n_keys;
  const int retval = RedisModule_StringToLongLong(argv[1], &n_keys);
  if (retval != REDISMODULE_OK || n_keys <= 0) {
    RedisModule_ReplyWithError(
        ctx,
        "ERR invalid or negative value found in number of keys to PERSIST");
    return -1;
  }

  int number_loaded_keys = 0;
  int separator_flag = 0;
  size_t argpos = 2;
  for (; (argpos < argc) && (number_loaded_keys < n_keys); argpos++) {
    const char *arg_string = RedisModule_StringPtrLen(argv[argpos], NULL);
    if (!strcasecmp(arg_string, chaining_operator)) {
      separator_flag = 1;
      break;
    } else {
      const char *key = RedisModule_Strdup(arg_string);
      AI_dictAdd(*persistContextDict, (void*)key, (void *)1);
      number_loaded_keys++;
    }
  }
  if (number_loaded_keys != n_keys) {
    RedisModule_WrongArity(ctx);
    return -1;
  }
  return argpos;
}

int RedisAI_DagRunSyntaxParser(RedisModuleCtx *ctx, RedisModuleString **argv,
                                 int argc, int dagMode) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  RedisAI_RunInfo *rinfo = NULL;
  if (RAI_InitRunInfo(&rinfo) == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(
        ctx,
        "ERR Unable to allocate the memory and initialise the RedisAI_RunInfo "
        "structure");
  }
  rinfo->use_local_context = 1;
  RAI_DagOp *currentDagOp = NULL;
  RAI_InitDagOp(&currentDagOp);
  array_append(rinfo->dagOps, currentDagOp);

  int persistFlag = 0;
  int loadFlag = 0;
  int chainingOpCount = 0;

  for (size_t argpos = 1; argpos <= argc - 1; argpos++) {
    const char *arg_string = RedisModule_StringPtrLen(argv[argpos], NULL);
    if (!strcasecmp(arg_string, "LOAD")) {
      loadFlag = 1;
      const int parse_result = RAI_parseDAGLoadArgs(
          ctx, &argv[argpos], argc - argpos, &(rinfo->dagTensorsLoadedContext),
          &(rinfo->dagTensorsContext), "|>");
      if (parse_result > 0) {
        argpos += parse_result - 1;
      } else {
        RAI_FreeRunInfo(ctx, rinfo);
        return REDISMODULE_ERR;
      }
    } else if (!strcasecmp(arg_string, "PERSIST")) {
      if (dagMode == REDISAI_DAG_READONLY_MODE) {
        RAI_FreeRunInfo(ctx, rinfo);
        return RedisModule_ReplyWithError(
            ctx, "ERR PERSIST cannot be specified in a read-only DAG");
      }
      persistFlag = 1;
      const int parse_result =
          RAI_parseDAGPersistArgs(ctx, &argv[argpos], argc - argpos,
                                  &(rinfo->dagTensorsPersistentContext), "|>");
      if (parse_result > 0) {
        argpos += parse_result - 1;
      } else {
        RAI_FreeRunInfo(ctx, rinfo);
        return REDISMODULE_ERR;
      }
    } else if (!strcasecmp(arg_string, "|>")) {
      // on the first pipe operator, if LOAD or PERSIST were used, we've already
      // allocated memory
      if (!((persistFlag == 1 || loadFlag == 1) && chainingOpCount == 0)) {
        rinfo->dagNumberCommands++;
        RAI_DagOp *currentDagOp = NULL;
        RAI_InitDagOp(&currentDagOp);
        array_append(rinfo->dagOps, currentDagOp);
      }
      chainingOpCount++;
    } else {
      if (!strcasecmp(arg_string, "AI.TENSORGET")) {
        rinfo->dagOps[rinfo->dagNumberCommands]->commandType =
            REDISAI_DAG_CMD_TENSORGET;
        rinfo->dagOps[rinfo->dagNumberCommands]->devicestr = "CPU";
      }
      if (!strcasecmp(arg_string, "AI.TENSORSET")) {
        rinfo->dagOps[rinfo->dagNumberCommands]->commandType =
            REDISAI_DAG_CMD_TENSORSET;
        rinfo->dagOps[rinfo->dagNumberCommands]->devicestr = "CPU";
      }
      if (!strcasecmp(arg_string, "AI.MODELRUN")) {
        if (argc - 2 < argpos) {
          return RedisModule_WrongArity(ctx);
        }
        RAI_DagOp *currentOp = rinfo->dagOps[rinfo->dagNumberCommands];
        currentOp->commandType = REDISAI_DAG_CMD_MODELRUN;
        RAI_Model *mto;
        RedisModuleKey *modelKey;
        const int status = RAI_GetModelFromKeyspace(
            ctx, argv[argpos + 1], &modelKey, &mto, REDISMODULE_READ);
        if (status == REDISMODULE_ERR) {
          RAI_FreeRunInfo(ctx, rinfo);
          return REDISMODULE_ERR;
        }
        currentOp->devicestr = mto->devicestr;
        // if (deviceStr == NULL) {
        //   deviceStr = mto->devicestr;
        // } else {
        //   // If the device strings are not equivalent, reply with error ( for
        //   // now )
        //   if (strcasecmp(mto->devicestr, deviceStr) != 0) {
        //     RAI_FreeRunInfo(ctx, rinfo);
        //     return RedisModule_ReplyWithError(
        //         ctx, "ERR multi-device DAGs not supported yet");
        //   }
        // }
        currentOp->runkey = argv[argpos + 1];
        currentOp->mctx = RAI_ModelRunCtxCreate(mto);
        // const int parse_result = RedisAI_Parse_ModelRun_RedisCommand(
        //     NULL, currentOp->argv, currentOp->argc, &(currentOp->mctx),
        //     &(currentOp->inkeys), &(currentOp->outkeys),
        //     &(currentOp->mctx->model), currentOp->err);
        // if (parse_result < 0) {
        //   printf("MODELRUN PARSING ERROR %s\n", currentOp->err->detail_oneline);
        //   return REDISMODULE_ERR;
        // }
        // else {
        //   argpos += parse_result - 1;
        // }
      }
      if (!strcasecmp(arg_string, "AI.SCRIPTRUN")) {
        if (argc - 3 < argpos) {
          return RedisModule_WrongArity(ctx);
        }
        RAI_DagOp *currentOp = rinfo->dagOps[rinfo->dagNumberCommands];
        currentOp->commandType = REDISAI_DAG_CMD_SCRIPTRUN;
        RAI_Script *sto;
        RedisModuleKey *scriptKey;
        const int status = RAI_GetScriptFromKeyspace(
            ctx, argv[argpos + 1], &scriptKey, &sto, REDISMODULE_READ);
        if (status == REDISMODULE_ERR) {
          RAI_FreeRunInfo(ctx, rinfo);
          return REDISMODULE_ERR;
        }
        // if (deviceStr == NULL) {
        //   deviceStr = sto->devicestr;
        // } else {
        //   // If the device strings are not equivalent, reply with error ( for
        //   // now )
        //   if (strcasecmp(sto->devicestr, deviceStr) != 0) {
        //     RAI_FreeRunInfo(ctx, rinfo);
        //     return RedisModule_ReplyWithError(
        //         ctx, "ERR multi-device DAGs not supported yet");
        //   }
        // }
        currentOp->devicestr = sto->devicestr;
        const char *functionName =
            RedisModule_StringPtrLen(argv[argpos + 2], NULL);
        currentOp->runkey = argv[argpos + 1];
        currentOp->sctx = RAI_ScriptRunCtxCreate(sto, functionName);
        // const int parse_result = RedisAI_Parse_ScriptRun_RedisCommand(
        //         NULL, currentOp->argv, currentOp->argc, &(currentOp->sctx),
        //         &(currentOp->inkeys), &(currentOp->outkeys),
        //         &(currentOp->sctx->script), currentOp->err);
        // if (parse_result < 0) {
        //   printf("SCRIPTRUN PARSING ERROR %s\n", currentOp->err->detail_oneline);
        //   return REDISMODULE_ERR;
        // }
        // else {
        //   argpos += parse_result - 1;
        // }
      }
      RedisModule_RetainString(NULL, argv[argpos]);
      rinfo->dagOps[rinfo->dagNumberCommands]->argv = array_append(rinfo->dagOps[rinfo->dagNumberCommands]->argv, argv[argpos]);
      rinfo->dagOps[rinfo->dagNumberCommands]->argc++;
    }
  }

  for (long long i=0; i<array_len(rinfo->dagOps); i++) {
    RAI_DagOp *currentOp = rinfo->dagOps[i];
    int parse_result;
    switch (currentOp->commandType) {
      case REDISAI_DAG_CMD_MODELRUN:
        parse_result = RedisAI_Parse_ModelRun_RedisCommand(
            NULL, currentOp->argv, currentOp->argc, &(currentOp->mctx),
            &(currentOp->inkeys), &(currentOp->outkeys),
            &(currentOp->mctx->model), currentOp->err);
        if (parse_result < 0) {
          return REDISMODULE_ERR;
        }
        break;
      case REDISAI_DAG_CMD_SCRIPTRUN:
        parse_result = RedisAI_Parse_ScriptRun_RedisCommand(
                NULL, currentOp->argv, currentOp->argc, &(currentOp->sctx),
                &(currentOp->inkeys), &(currentOp->outkeys),
                &(currentOp->sctx->script), currentOp->err);
        if (parse_result < 0) {
          return REDISMODULE_ERR;
        }
        break;
    }
  }
 
  // DONE DAG
  // Now clone RunInfo as needed, equip it with appropriate mutex locks
  // and push it to the appropriate devices

  // If there was no MODELRUN or SCRIPTRUN on the DAG, we default all ops to CPU
  // if (deviceStr == NULL) {
  //   deviceStr = "CPU";
  // }
  // If the queue does not exist, initialize it

  rinfo->client = RedisModule_BlockClient(ctx, RedisAI_DagRun_Reply, NULL, NULL, 0);

  for (long long i=0; i<array_len(rinfo->dagOps); i++) {
    if (rinfo->dagOps[i]->devicestr == NULL) {
      rinfo->dagOps[i]->devicestr = "CPU";
    }
  }

  const char **devices = array_new(const char *, 10);

  for (long long i=0; i<array_len(rinfo->dagOps); i++) {
    const char* devicestr = rinfo->dagOps[i]->devicestr;
    bool found = false;
    for (long long j=0; j<array_len(devices); j++) {
      if (strcmp(devicestr, devices[j]) == 0) {
        found = true;
        break;
      }
    }
    if (!found) {
      devices = array_append(devices, devicestr);
    }
  }
  
  const char* master_device = rinfo->dagOps[array_len(rinfo->dagOps)-1]->devicestr;

  RedisAI_RunInfo **rinfo_copies = array_new(RedisAI_RunInfo*, 10);
  
  for (long long i=0; i<array_len(devices)-1; i++) {
    RedisAI_RunInfo *rinfo_copy;
    RAI_ShallowCopyDagRunInfo(&rinfo_copy, rinfo);
    rinfo_copies = array_append(rinfo_copies, rinfo_copy);
  }

  int copy_count = 0;
  for (long long i=0; i<array_len(devices); i++) {
    const char* devicestr = devices[i];
    RunQueueInfo *run_queue_info = NULL;
    if (ensureRunQueue(devicestr, &run_queue_info) == REDISMODULE_ERR) {
      RAI_FreeRunInfo(ctx, rinfo);
      return RedisModule_ReplyWithError(ctx,
                                        "ERR Queue not initialized for device");
    }

    RedisAI_RunInfo *curr_rinfo = NULL;

    if (!strcasecmp(devicestr, master_device)) {
      curr_rinfo = rinfo;
    }
    else {
      curr_rinfo = rinfo_copies[copy_count];
      copy_count += 1;
    }

    printf("PRE PUSH\n");

    pthread_mutex_lock(&run_queue_info->run_queue_mutex);
    queuePush(run_queue_info->run_queue, curr_rinfo);
    pthread_cond_signal(&run_queue_info->queue_condition_var);
    pthread_mutex_unlock(&run_queue_info->run_queue_mutex);
  }

  array_free(devices);
  array_free(rinfo_copies);

  return REDISMODULE_OK;
}