/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.VectorizationDetailLevel;
import org.apache.hadoop.hive.ql.plan.ExplainWork;

/**
 * ExplainSemanticAnalyzer.
 *
 */
public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {
  List<FieldSchema> fieldList;
  ExplainConfiguration config;

  public ExplainSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    config = new ExplainConfiguration();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    final int childCount = ast.getChildCount();
    int i = 1;   // Skip TOK_QUERY.
    while (i < childCount) {
      int explainOptions = ast.getChild(i).getType();
      if (explainOptions == HiveParser.KW_FORMATTED) {
        config.setFormatted(true);
      } else if (explainOptions == HiveParser.KW_EXTENDED) {
        config.setExtended(true);
      } else if (explainOptions == HiveParser.KW_DEPENDENCY) {
        config.setDependency(true);
      } else if (explainOptions == HiveParser.KW_LOGICAL) {
        config.setLogical(true);
      } else if (explainOptions == HiveParser.KW_AUTHORIZATION) {
        config.setAuthorize(true);
      } else if (explainOptions == HiveParser.KW_VECTORIZATION) {
        config.setVectorization(true);
        if (i + 1 < childCount) {
          int vectorizationOption = ast.getChild(i + 1).getType();

          // [ONLY]
          if (vectorizationOption == HiveParser.TOK_ONLY) {
            config.setVectorizationOnly(true);
            i++;
            if (i + 1 >= childCount) {
              break;
            }
            vectorizationOption = ast.getChild(i + 1).getType();
          }

          // [SUMMARY|OPERATOR|EXPRESSION|DETAIL]
          if (vectorizationOption == HiveParser.TOK_SUMMARY) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.SUMMARY);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_OPERATOR) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.OPERATOR);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_EXPRESSION) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.EXPRESSION);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_DETAIL) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.DETAIL);
            i++;
          }
        }
      } else {
        // UNDONE: UNKNOWN OPTION?
      }
      i++;
    }

    ctx.setExplain(true);
    ctx.setExplainLogical(config.isLogical());

    // Create a semantic analyzer for the query
    ASTNode input = (ASTNode) ast.getChild(0);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, input);
    sem.analyze(input, ctx);
    sem.validate();

    ctx.setResFile(ctx.getLocalTmpPath());
    List<Task<? extends Serializable>> tasks = sem.getAllRootTasks();
    if (tasks == null) {
      tasks = Collections.emptyList();
    }

    FetchTask fetchTask = sem.getFetchTask();
    if (fetchTask != null) {
      // Initialize fetch work such that operator tree will be constructed.
      fetchTask.getWork().initializeForFetch(ctx.getOpContext());
    }

    ParseContext pCtx = null;
    if (sem instanceof SemanticAnalyzer) {
      pCtx = ((SemanticAnalyzer)sem).getParseContext();
    }

    config.setUserLevelExplain(!config.isExtended()
        && !config.isFormatted()
        && !config.isDependency()
        && !config.isLogical()
        && !config.isAuthorize()
        && (HiveConf.getBoolVar(ctx.getConf(), HiveConf.ConfVars.HIVE_EXPLAIN_USER) && HiveConf
            .getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")));
    ExplainWork work = new ExplainWork(ctx.getResFile(),
        pCtx,
        tasks,
        fetchTask,
        sem,
        config,
        ctx.getCboInfo());

    work.setAppendTaskType(
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEEXPLAINDEPENDENCYAPPENDTASKTYPES));

    ExplainTask explTask = (ExplainTask) TaskFactory.get(work, conf);

    fieldList = explTask.getResultSchema();
    rootTasks.add(explTask);
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return fieldList;
  }

  @Override
  public boolean skipAuthorization() {
    List<Task<? extends Serializable>> rootTasks = getRootTasks();
    assert rootTasks != null && rootTasks.size() == 1;
    Task task = rootTasks.get(0);
    return task instanceof ExplainTask && ((ExplainTask)task).getWork().isAuthorize();
  }
}
