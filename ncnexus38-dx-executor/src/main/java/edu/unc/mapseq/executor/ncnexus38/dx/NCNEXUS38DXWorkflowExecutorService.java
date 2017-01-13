package edu.unc.mapseq.executor.ncnexus38.dx;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCNEXUS38DXWorkflowExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(NCNEXUS38DXWorkflowExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCNEXUS38DXWorkflowExecutorTask task;

    private Long period = 5L;

    public NCNEXUS38DXWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000; // 1 minute
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCNEXUS38DXWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NCNEXUS38DXWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
