package edu.unc.mapseq.commands.ncnexus38.dx;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncnexus38.dx.AssertExpectedOutputFilesExistInIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Sample;

@Command(scope = "ncnexus38-dx", name = "assert-expected-output-files-exist-in-irods")
@Service
public class AssertExpectedOutputFilesExistInIRODSAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(AssertExpectedOutputFilesExistInIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Option(name = "--sampleId", required = true, multiValued = false)
    private Long sampleId;

    @Option(name = "--version", required = true, multiValued = false)
    private String version;

    @Option(name = "--dx", required = true, multiValued = false)
    private String dx;

    @Override
    public Object execute() throws Exception {
        logger.debug("ENTERING execute()");
        ExecutorService es = Executors.newSingleThreadExecutor();
        Sample sample = maPSeqDAOBeanService.getSampleDAO().findById(sampleId);
        AssertExpectedOutputFilesExistInIRODSRunnable runnable = new AssertExpectedOutputFilesExistInIRODSRunnable(maPSeqDAOBeanService,
                sample, version, dx);
        es.submit(runnable);
        es.shutdown();
        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
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
