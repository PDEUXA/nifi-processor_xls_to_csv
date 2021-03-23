/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bl.data.nifi.processors.xls2csv;



import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.*;

import org.apache.poi.ss.usermodel.*;

import java.io.IOException;
import java.util.*;

@Tags({"excel,csv"})
@CapabilityDescription("Convert a single sheet excel document into a csv")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "sheetname", description = "Name of the converted sheet.")})

public class xls2csv extends AbstractProcessor {

    public static final String SHEET_NAME = "sheetname";

    public static final PropertyDescriptor SHEET_NUMBER = new PropertyDescriptor
            .Builder().name("SHEET_NUMBER")
            .displayName("Sheet Number")
            .description("Position of the sheet to be extracted, first sheet is 0")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALL_SHEET = new PropertyDescriptor
            .Builder().name("ALL_SHEET")
            .displayName("Get All sheet from the excel file")
            .description("True or False")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor ROW_OFFSET = new PropertyDescriptor
            .Builder().name("ROW_OFFSET")
            .displayName("Number of Rows to Skip")
            .description("The row number of the first row to start processing."
                    + "Use this to skip over rows of data at the top of your worksheet that are not part of the dataset."
                    + "Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLUMN_OFFSET = new PropertyDescriptor
            .Builder().name("COLUMN_OFFSET")
            .displayName("Number of Columns To Skip")
            .description("The column number of the first column to start processing."
                    + "Use this to skip over rows of data at the top of your worksheet that are not part of the dataset."
                    + "Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_SEPARATOR = new PropertyDescriptor
            .Builder().name("CSV_SEPARATOR")
            .displayName("Separator used in the output CSV file")
            .description("Separator used in the output CSV file")
            .required(true)
            .defaultValue(",")
            .allowableValues(",",";","\t")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Conversion successful")
            .build();


    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Conversion failed")
            .build();

    public static final Relationship REL_ORIGNE = new Relationship.Builder()
            .name("Origine")
            .description("Original Flowfile")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SHEET_NUMBER);
        descriptors.add(COLUMN_OFFSET);
        descriptors.add(ROW_OFFSET);
        descriptors.add(ALL_SHEET);
        descriptors.add(CSV_SEPARATOR);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGNE);

        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        Boolean getAllSheet = context.getProperty(ALL_SHEET).asBoolean();
        int rowNum = context.getProperty(ROW_OFFSET).asInteger();
        int colNum = context.getProperty(COLUMN_OFFSET).asInteger();
        int sheetNumber = context.getProperty(SHEET_NUMBER).asInteger();
        String csvSeparator = context.getProperty(CSV_SEPARATOR).getValue();
        ;
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                Workbook wb = WorkbookFactory.create(inputStream);
                if (getAllSheet) {
                    int numberOfSheets = wb.getNumberOfSheets();
                    for (int i = 0; i < numberOfSheets; i++) {
                        FlowFile ff = session.create(flowFile);
                        String sheetName = wb.getSheetAt(i).getSheetName();
                        try {
                            handleExcelSheet(session, ff, wb, i, rowNum, colNum, csvSeparator);
                            session.putAttribute(ff, SHEET_NAME, sheetName);
                            session.transfer(ff, REL_SUCCESS);

                        } catch (IOException e) {
                            session.putAttribute(ff, SHEET_NAME, sheetName);
                            session.transfer(ff, REL_FAILURE);
                        }
                    }
                } else {
                    FlowFile ff = session.create(flowFile);
                    String sheetName = wb.getSheetAt(sheetNumber).getSheetName();
                    try {
                        handleExcelSheet(session, ff, wb, sheetNumber, rowNum, colNum, csvSeparator);
                        session.putAttribute(ff, SHEET_NAME, sheetName);
                        session.transfer(ff, REL_SUCCESS);
                    } catch (IOException e) {
                        session.putAttribute(ff, SHEET_NAME, sheetName);
                        session.transfer(ff, REL_FAILURE);
                    }
                }
            }
        });
        session.transfer(flowFile, REL_ORIGNE);
    }

    private void handleExcelSheet(ProcessSession session, FlowFile flowFile, Workbook wb,
                                  final int sheetNumber, final int rowOffset, final int colOffset, final String csvSeparator) throws IOException {
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
                Sheet mySheet = wb.getSheetAt(sheetNumber);
                StringBuilder record = new StringBuilder();
                Iterator<Row> rowIter = mySheet.rowIterator();
                for (int i = 0; i < rowOffset - 1; i++) {
                    if (rowIter.hasNext()) {
                        rowIter.next();
                    }
                }
                boolean isRow;
                while (rowIter.hasNext()) {
                    isRow = true;
                    Row nextRow = rowIter.next();
                    int lastCell = nextRow.getLastCellNum();
                    for (int i = colOffset; i < lastCell; i++) {
                        DataFormatter dataFormatter = new DataFormatter();
                        String value = dataFormatter.formatCellValue(nextRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK));
                        record.append(value);
                        if (i != lastCell - 1) {
                            record.append(csvSeparator);
                        }
                    }
                    if (isRow) {
                        for (int index = 0; index < record.length(); index++) {
                            if (record.charAt(index) == '\n') {
                                record.setCharAt(index, ' ');
                            }
                        }
                        record = new StringBuilder(record + "\n");
                        outputStream.write(record.toString().getBytes(StandardCharsets.UTF_8));
                    }
                    record = new StringBuilder();
                }
            }
        });
        String filename = flowFile.getAttribute("filename") + ".csv";
        flowFile = session.putAttribute(flowFile, "filename", filename);
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "text/csv");

    }
}


