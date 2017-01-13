package edu.unc.mapseq.commons.ncnexus38.dx;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class AssertExpectedOutputFilesExistInIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AssertExpectedOutputFilesExistInIRODSRunnable.class);

    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    private Sample sample;

    private String version;

    private String dx;

    public AssertExpectedOutputFilesExistInIRODSRunnable(MaPSeqDAOBeanService maPSeqDAOBeanService, Sample sample, String version,
            String dx) {
        super();
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
        this.sample = sample;
        this.version = version;
        this.dx = dx;
    }

    @Override
    public void run() {
        logger.debug("ENTERING run()");

        Workflow workflow = null;
        try {
            workflow = maPSeqDAOBeanService.getWorkflowDAO().findByName("NCNEXUSDX").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        File outputDirectory = SequencingWorkflowUtil.createOutputDirectory(sample, workflow);
        File tmpDir = new File(outputDirectory, "tmp");
        if (!tmpDir.exists()) {
            tmpDir.mkdirs();
        }

        String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncnexus/analysis/%s/L%03d_%s/%s/%s",
                workflow.getSystem().getValue(), sample.getFlowcell().getName(), sample.getLaneIndex(), sample.getBarcode(),
                workflow.getName(), version);

        List<CommandInput> commandInputList = new LinkedList<CommandInput>();

        CommandOutput commandOutput = null;

        String rootFileName = String.format("%s_%s_L%03d.fixed-rg.deduped.realign.fixmate.recal", sample.getFlowcell().getName(),
                sample.getBarcode(), sample.getLaneIndex());

        File sampleCumulativeCoverageCountsFile = new File(outputDirectory,
                String.format("%s.coverage.v%s.gene.sample_cumulative_coverage_counts", rootFileName, version));

        File sampleCumulativeCoverageProportionsFile = new File(outputDirectory,
                String.format("%s.coverage.v%s.gene.sample_cumulative_coverage_proportions", rootFileName, version));
        File sampleIntervalStatsFile = new File(outputDirectory,
                String.format("%s.coverage.v%s.gene.sample_interval_statistics", rootFileName, version));
        File sampleIntervalSummaryFile = new File(outputDirectory,
                String.format("%s.coverage.v%s.gene.sample_interval_summary", rootFileName, version));
        File sampleStatsFile = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_statistics", rootFileName, version));
        File sampleSummaryFile = new File(outputDirectory, String.format("%s.coverage.v%s.gene.sample_summary", rootFileName, version));
        File filteredBAMFile = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.bam", rootFileName, dx, version));
        File filteredSortedBAMFile = new File(outputDirectory,
                String.format("%s.filtered_by_dxid_%s_v%s.sorted.bam", rootFileName, dx, version));
        File filteredSortedBAIFile = new File(outputDirectory,
                String.format("%s.filtered_by_dxid_%s_v%s.sorted.bai", rootFileName, dx, version));
        File zipFile = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.sorted.zip", rootFileName, dx, version));
        File vcfFile = new File(outputDirectory, String.format("%s.filtered_by_dxid_%s_v%s.vcf", rootFileName, dx, version));

        Arrays.asList(sampleCumulativeCoverageCountsFile, sampleCumulativeCoverageProportionsFile, sampleIntervalStatsFile,
                sampleIntervalSummaryFile, sampleStatsFile, sampleSummaryFile, filteredBAMFile, filteredSortedBAMFile,
                filteredSortedBAIFile, zipFile, vcfFile)
                .forEach(a -> commandInputList
                        .add(new CommandInput(String.format("$IRODS_HOME/ils %s/%s", irodsDirectory, a.getName()), tmpDir, Boolean.FALSE)));

        File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
        Executor executor = BashExecutor.getInstance();

        for (CommandInput ci : commandInputList) {
            try {
                commandOutput = executor.execute(ci, mapseqrc);
                if (commandOutput.getExitCode() != 0) {
                    logger.warn(ci.getCommand().split(" ")[1]);
                }
            } catch (ExecutorException e) {
                if (commandOutput != null) {
                    logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                }
            }
        }

    }

    public MaPSeqDAOBeanService getMaPSeqDAOBeanService() {
        return maPSeqDAOBeanService;
    }

    public void setMaPSeqDAOBeanService(MaPSeqDAOBeanService maPSeqDAOBeanService) {
        this.maPSeqDAOBeanService = maPSeqDAOBeanService;
    }

    public Sample getSample() {
        return sample;
    }

    public void setSample(Sample sample) {
        this.sample = sample;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDx() {
        return dx;
    }

    public void setDx(String dx) {
        this.dx = dx;
    }

}
