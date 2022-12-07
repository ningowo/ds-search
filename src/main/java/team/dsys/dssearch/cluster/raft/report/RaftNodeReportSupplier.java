package team.dsys.dssearch.cluster.raft.report;

import io.microraft.report.RaftNodeReport;
import io.microraft.report.RaftNodeReportListener;

import java.util.function.Supplier;

public interface RaftNodeReportSupplier extends RaftNodeReportListener, Supplier<RaftNodeReport> {

}
