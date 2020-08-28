package de.metanome.algorithms.hyfd.mocks;

import com.github.codelionx.distod.DistodAlgorithm;
import com.github.codelionx.distod.DistodParameters;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.config.Config;
import de.metanome.algorithms.hyfd.utils.FileUtils;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class MetanomeMock {

    public static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();
        for (String columnName : relationalInput.columnNames())
            acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
        return acceptedColumns;
    }

    public static void executeDistod(Config conf) {
        try {
            DistodAlgorithm distod = new DistodAlgorithm();

            ArrayList<ConfigurationRequirement<?>> reqs = distod.getConfigurationRequirements();
            System.out.println("Requirements: " + reqs);

//            System.out.println("Filepath: " + new File(conf.inputFolderPath + conf.inputDatasetName + conf.inputFileEnding).getAbsolutePath());
            RelationalInputGenerator relationalInputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                    conf.inputFolderPath + conf.inputDatasetName + conf.inputFileEnding, true,
                    conf.inputFileSeparator, conf.inputFileQuotechar, conf.inputFileEscape, conf.inputFileStrictQuotes,
                    conf.inputFileIgnoreLeadingWhiteSpace, conf.inputFileSkipLines, conf.inputFileHasHeader,
					conf.inputFileSkipDifferingLines, conf.inputFileNullString));

            ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(relationalInputGenerator));
            //ResultReceiver resultReceiver = new ResultCounter("MetanomeMock", getAcceptedColumns(relationalInputGenerator));

            RelationalInputGenerator[] gens = new RelationalInputGenerator[1];
            gens[0] = relationalInputGenerator;
            distod.setRelationalInputConfigurationValue(DistodAlgorithm.inputGeneratorIdentifier(), gens);
            String[] hosts = new String[1];
            hosts[0] = DistodParameters.Host$.MODULE$.defaultValue();
            distod.setStringConfigurationValue(DistodParameters.Host$.MODULE$.identifier(), hosts);
//            hyFD.setBooleanConfigurationValue(HyFD.Identifier.NULL_EQUALS_NULL.name(), Boolean.valueOf(conf.nullEqualsNull));
//            hyFD.setBooleanConfigurationValue(HyFD.Identifier.VALIDATE_PARALLEL.name(), Boolean.valueOf(conf.validateParallel));
//            hyFD.setBooleanConfigurationValue(HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Boolean.valueOf(conf.enableMemoryGuardian));
//            hyFD.setIntegerConfigurationValue(HyFD.Identifier.MAX_DETERMINANT_SIZE.name(), Integer.valueOf(conf.maxDeterminantSize));
            distod.setResultReceiver(resultReceiver);

            long time = System.currentTimeMillis();
            distod.execute();
            time = System.currentTimeMillis() - time;

            if (conf.writeResults) {
                String outputPath = conf.measurementsFolderPath + conf.inputDatasetName + File.separator;
                List<Result> results;
                results = resultReceiver.fetchNewResults();

                FileUtils.writeToFile(
                        distod.toString() + "\r\n\r\n" +
                                "Runtime: " + time + "\r\n\r\n" +
                                "Results: " + results.size() + "\r\n\r\n" +
                                conf.toString(), outputPath + conf.statisticsFileName);
                FileUtils.writeToFile(format(results), outputPath + conf.resultFileName);
            }
        } catch (AlgorithmExecutionException | IOException e) {
            e.printStackTrace();
        }
    }

    private static String format(List<Result> results) {
        HashMap<String, List<String>> lhs2rhs = new HashMap<String, List<String>>();

        for (Result result : results) {
            FunctionalDependency fd = (FunctionalDependency) result;

            StringBuilder lhsBuilder = new StringBuilder("[");
            Iterator<ColumnIdentifier> iterator = fd.getDeterminant().getColumnIdentifiers().iterator();
            while (iterator.hasNext()) {
                lhsBuilder.append(iterator.next().toString());
                if (iterator.hasNext())
                    lhsBuilder.append(", ");
            }
            lhsBuilder.append("]");
            String lhs = lhsBuilder.toString();

            String rhs = fd.getDependant().toString();

            if (!lhs2rhs.containsKey(lhs))
                lhs2rhs.put(lhs, new ArrayList<String>());
            lhs2rhs.get(lhs).add(rhs);
        }

        StringBuilder builder = new StringBuilder();
        ArrayList<String> lhss = new ArrayList<String>(lhs2rhs.keySet());
        Collections.sort(lhss);
        for (String lhs : lhss) {
            List<String> rhss = lhs2rhs.get(lhs);
            Collections.sort(rhss);

            if (rhss.isEmpty())
                continue;

            builder.append(lhs).append(" --> ");
            builder.append(rhss);
            builder.append("\r\n");
        }
        return builder.toString();
    }
}
