/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.taskmanager.server;

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Should be thread-safe
 * We'll save job result in TaskManagerConfig.JOB_LOG_PATH/tmp_result, not
 * offline storage path,
 * cuz we just want result restored in local file system.
 */
@Slf4j
public class JobResultSaver {
    // false: unused, true: using
    // 0: unused, 1: saving, 2: finished but still in use
    private List<Integer> idStatus;

    public JobResultSaver() {
        idStatus = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(128, 0)));
    }

    /**
     * Generate unique id for job result, != spark job id
     * We generate id before submit the spark job, so don't worry about saveFile for
     * result id when result id status==0
     */
    public int genResultId() {
        int id;
        // atomic
        synchronized (idStatus) {
            id = idStatus.indexOf(0);
            if (id == -1) {
                throw new RuntimeException("too much running jobs to save job result, reject this spark job");
            }
            idStatus.set(id, 1);
        }
        return id;
    }

    public String genUniqueFileName() {
        String file;
        synchronized (this) {
            file = UUID.randomUUID().toString();
        }
        return String.format("%s.csv", file);
    }

    public boolean saveFile(int resultId, String jsonData) {
        // No need to wait, cuz id status must have been changed by genResultId before.
        // It's a check.
        int status = idStatus.get(resultId);
        if (status != 1) {
            throw new RuntimeException(
                    String.format("why send to not running save job %d(status %d)", resultId, status));
        }
        if (jsonData.isEmpty()) {
            synchronized (idStatus) {
                idStatus.set(resultId, 2);
                idStatus.notifyAll();
            }
            log.info("saved all result of result " + resultId);
        }
        // save to <log path>/tmp_result/<result_id>/<unique file name>
        try {
            String savePath = String.format("%s/tmp_result/%d", TaskManagerConfig.JOB_LOG_PATH, resultId);
            synchronized (this) {
                File saveP = new File(savePath);
                if (!saveP.exists()) {
                    boolean res = saveP.mkdirs();
                    log.info("create save path " + savePath + ", status " + res);
                }
            }
            String fileFullPath = String.format("%s/%s", savePath, genUniqueFileName());
            File resultFile = new File(fileFullPath);
            if (!resultFile.createNewFile()) {
                throw new RuntimeException("job result file exsits");
            }

            FileWriter wr = new FileWriter(fileFullPath);
            wr.write(jsonData);

            wr.flush();
            wr.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String readResult(int resultId) throws InterruptedException {
        // wait for idStatus[resultId] == 2
        synchronized (idStatus) {
            while (idStatus.get(resultId) != 2) {
                idStatus.wait();
            }
        }
        // all finished, read csv from savePath
        String savePath = String.format("%s/tmp_result/%d", TaskManagerConfig.JOB_LOG_PATH, resultId);
        String output = printFilesTostr(savePath);

        // cleanup dir
        new File(savePath).delete();

        // reset id
        synchronized (idStatus) {
            idStatus.set(resultId, 0);
            idStatus.notifyAll();
        }
        return output;
    }

    private String printFilesTostr(String fileDir) {
        StringWriter stringWriter = new StringWriter();
        try (Stream<Path> paths = Files.walk(Paths.get(fileDir))) {
            List<String> csvFiles = paths.filter(Files::isRegularFile).map(f -> f.toString()).filter(f -> f.endsWith(".csv"))
                .collect(Collectors.toList());
            if(csvFiles.isEmpty()) {
                log.warn("no result file saved");
                return "";
            }
            CSVFormat format = CSVFormat.Builder.create().setHeader().build();
            // get the header by peek the first file
            CSVPrinter csvPrinter = new CSVPrinter(stringWriter, CSVFormat.DEFAULT.withHeader(
                CSVParser.parse(csvFiles.get(0), format).getHeaderNames().stream().toArray(String[]::new)));
            for(String f: csvFiles) {
                CSVParser parser = CSVParser.parse(f, format);
                Iterator<CSVRecord> iter = parser.iterator();
                while(iter.hasNext()) {
                    csvPrinter.printRecord(iter.next());
                }
            }
        } 
        return stringWriter.toString();
    }
}
