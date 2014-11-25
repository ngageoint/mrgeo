package org.mrgeo.pdf;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class PdfHistogramBuilderIntegrationTest extends LocalRunnerTest
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(PdfHistogramBuilderIntegrationTest.class);


  private static boolean _isClose(double a, double b)
  {
    return Math.abs(a - b) < 1e-5;
  }

  private void _verifyPdfCurve(Path pdfPath, File pdfExpectedFile)
  {
    PdfCurve pdfCurve = null;
    try
    {
      pdfCurve = PdfFactory.loadPdf(pdfPath, conf);

      FileReader fr = new FileReader(pdfExpectedFile);
      BufferedReader r = new BufferedReader(fr);
      try
      {
        String line = r.readLine();
        while (line != null)
        {
          String[] inputs = line.split(",");
          Assert.assertEquals("Expected results should contain two values: " + line, 2, inputs.length);
          double expectedLikelihood = Double.parseDouble(inputs[1]);
          double actualLikelihood = pdfCurve.getLikelihood(Double.parseDouble(inputs[0]));
          Assert.assertTrue("Expected: " + expectedLikelihood + ", actual: " + actualLikelihood + ", at rfd: " + inputs[0],
            _isClose(actualLikelihood, expectedLikelihood));
          line = r.readLine();
        }
      }
      finally
      {
        r.close();
        fr.close();
      }
    }
    catch(Exception e)
    {
      Assert.fail("Unexpected exception: " + e);
    }

  }

  @Test
  @Category(IntegrationTest.class)
  public void testBasics() throws IOException, IllegalArgumentException, JobFailedException, JobCancelledException 
  {
    Path outputHdfs = TestUtils.composeOutputHdfs(PdfHistogramBuilderIntegrationTest.class);
    String input = TestUtils.composeInputDir(PdfHistogramBuilderIntegrationTest.class);

    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv");
    HadoopFileUtils.copyToHdfs(input, outputHdfs, "input.tsv.columns");

    Path rfd = new Path(outputHdfs, "input.tsv");
    Path output = new Path(outputHdfs, "output");
    FileSystem fs = output.getFileSystem(this.conf);
    String factorName1 = "value1";
    String factorName2 = "value2";
    PdfHistogramBuilder.buildPdfs(fs, this.conf, rfd, new String[] { factorName1, factorName2 }, output, new ProgressHierarchy(), null);
    // Check the result. The pdf_results.txt file was gotten from exporting the same
    // PDF from Signature Analyst.
    _verifyPdfCurve(new Path(output, factorName1), new File(input, "pdf_results.csv"));
    // The input RFD's for both factors are the same, so the results should be the same.
    _verifyPdfCurve(new Path(output, factorName2), new File(input, "pdf_results.csv"));
  }
}
