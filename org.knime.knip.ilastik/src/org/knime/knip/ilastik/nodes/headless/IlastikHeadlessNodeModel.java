/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2015
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 */
package org.knime.knip.ilastik.nodes.headless;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.InvalidPathException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.FileUtil;
import org.knime.core.util.Pair;
import org.knime.knip.base.data.img.ImgPlusCell;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.base.data.img.ImgPlusValue;
import org.knime.knip.base.node.NodeUtils;
import org.knime.knip.core.KNIPGateway;
import org.knime.knip.ilastik.nodes.IlastikPreferencePage;
import org.knime.knip.io.ScifioImgSource;
import org.knime.knip.io.nodes.imgwriter2.ImgWriter2;
import org.scijava.log.DefaultUncaughtExceptionHandler;

import net.imagej.ImgPlus;
import net.imagej.ImgPlusMetadata;
import net.imagej.axis.Axes;
import net.imglib2.type.numeric.RealType;

/**
 *
 * Headless execution of an Ilastik project in a KNIME node.
 *
 * @author Andreas Graumann, University of Konstanz
 * @param <T>
 */
public class IlastikHeadlessNodeModel<T extends RealType<T>> extends NodeModel implements BufferedDataTableHolder {

    private static final String COL_NAME = "Result";

    static final class ColCreationModes {
        private ColCreationModes() {
            // NB Util Class
        }

        public static final String NEW_TABLE = "New Table";

        public static final String APPEND = "Append";

        public static final String REPLACE = "Replace";
    }

    /**
     * Path to ilastik project file
     */
    private SettingsModelString m_pathToIlastikProjectFileModel = createPathToIlastikProjectFileModel();

    /**
     * Source image column
     */
    private final SettingsModelString m_srcImgCol = createImgColModel();

    private final SettingsModelString m_colCreationModeModel = createColCreationModeModel();

    /**
     * ilastik cpu / memory limits
     */
    private final SettingsModelInteger m_ilastikMaxMemory = createIlastikMaxMemoryModel();
    private final SettingsModelIntegerBounded m_ilastikThreadCount = createIlastikThreadCountModel();

    /**
     * data table for table cell view
     */
    private BufferedDataTable m_data;

    private Map<RowKey, Pair<String, String>> m_outFiles;

    private ImgPlusCellFactory m_imgPlusCellFactory;

    /**
     * logger
     */

    private final NodeLogger LOGGER = NodeLogger.getLogger(NodeModel.class);

    /**
     * @param nrInDataPorts
     * @param nrOutDataPorts
     */
    protected IlastikHeadlessNodeModel(final int nrInDataPorts, final int nrOutDataPorts) {
        super(nrInDataPorts, nrOutDataPorts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        DataTableSpec outSpec;
        String colCreationMode = m_colCreationModeModel.getStringValue();

        if (colCreationMode.equals(ColCreationModes.NEW_TABLE)) { // new table
            outSpec = createImgSpec();
        } else if (colCreationMode.equals(ColCreationModes.APPEND)) { // Append
            final ColumnRearranger rearranger = new ColumnRearranger(inSpecs[0]);
            final DataColumnSpec newColSpec = new DataColumnSpecCreator(COL_NAME, ImgPlusCell.TYPE).createSpec();
            final IlastikCellFactory fac = createResultCellFactory(newColSpec);
            rearranger.append(fac);
            outSpec = rearranger.createSpec();
        } else if (colCreationMode.equals(ColCreationModes.REPLACE)) { // Replace
            final ColumnRearranger rearranger = new ColumnRearranger(inSpecs[0]);
            final DataColumnSpec newColSpec = inSpecs[0].getColumnSpec(m_srcImgCol.getStringValue());
            final IlastikCellFactory fac = createResultCellFactory(newColSpec);
            rearranger.replace(fac, m_srcImgCol.getStringValue());
            outSpec = rearranger.createSpec();
        } else {
            throw new IllegalArgumentException("The value of the column creation setting is invalid!");
        }

        return new DataTableSpec[]{outSpec};
    }

    /**
     * @return
     */
    private DataTableSpec createImgSpec() {
        return new DataTableSpec(new DataColumnSpecCreator(COL_NAME, ImgPlusCell.TYPE).createSpec());
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {

        // get index of image column
        int idx = getImgColIdx(inData[0].getSpec());

        // create tmp directory
        final String tmpDirPath = KNIMEConstants.getKNIMETempDir() + "/ilastik/" + UUID.randomUUID() + "/";
        final File tmpDir = new File(tmpDirPath);
        tmpDir.mkdirs();

        // list to store all input file names
        final List<String> files = new ArrayList<>();

        final ImgWriter2 iW = new ImgWriter2();

        m_outFiles = new LinkedHashMap<>();
        m_imgPlusCellFactory = new ImgPlusCellFactory(exec);

        int uniqueFileNameCounter = 0;

        // iterate over all input images and copy them to the tmp directory
        for (DataRow row : inData[0]) {

            final DataCell cell = row.getCell(idx);

            if (cell.isMissing()) {
                KNIPGateway.log().warn("Ignoring missing cell in row " + row.getKey() + "!");
                continue;
            }

            // get next image
            final ImgPlusValue<?> imgvalue = (ImgPlusValue<?>)cell;

            // create new unique file names
            String fileName = tmpDirPath + "file" + uniqueFileNameCounter;
            uniqueFileNameCounter++;

            // store out-image name in iterable
            // store in map
            String resultFileName = fileName + "_result.tif";

            m_outFiles.put(row.getKey(), new Pair<>(resultFileName, imgvalue.getImgPlus().getSource()));

            // attach file extension
            fileName = fileName + ".tif";

            // store in-image name in list
            files.add(fileName);

            // map for dimensions ZCT. -1 means non-existent
            final ImgPlusMetadata imgMeta = imgvalue.getMetadata();
            final int[] map = new int[] {
                    imgMeta.dimensionIndex(Axes.Z),
                    imgMeta.dimensionIndex(Axes.CHANNEL),
                    imgMeta.dimensionIndex(Axes.TIME)
            };

            for (int i = 0; i < map.length; i++) {
                if (map[i] != -1) {
                    map[i] -= 2; // substract 2 as we start _behind_ XY
                }
            }

            // Image Writer
            exec.checkCanceled();

            // write image to temp folder as input for ilastik
            iW.writeImage(imgvalue.getImgPlus(), fileName, "TIFF (tif)", "Uncompressed", map);
        }

        try {
            // run ilastik and process images
            runIlastik(tmpDirPath, files, exec);

            String colCreationMode = m_colCreationModeModel.getStringValue();

            if (colCreationMode.equals(ColCreationModes.NEW_TABLE)) { // new table

                BufferedDataContainer container = exec.createDataContainer(createImgSpec());
                readResultImages(new ImgPlusCellFactory(exec), container);
                container.close();
                m_data = container.getTable();
            } else if (colCreationMode.equals(ColCreationModes.APPEND)) { // Append
                final ColumnRearranger rearranger = new ColumnRearranger(inData[0].getSpec());
                final DataColumnSpec newColSpec = new DataColumnSpecCreator(COL_NAME, ImgPlusCell.TYPE).createSpec();
                final IlastikCellFactory fac = createResultCellFactory(newColSpec);
                rearranger.append(fac);
                m_data = exec.createColumnRearrangeTable(inData[0], rearranger, exec);
                fac.closeImgOpener();
            } else if (colCreationMode.equals(ColCreationModes.REPLACE)) { // Replace
                final ColumnRearranger rearranger = new ColumnRearranger(inData[0].getSpec());
                final DataColumnSpec newColSpec = inData[0].getSpec().getColumnSpec(m_srcImgCol.getStringValue());
                final IlastikCellFactory fac = createResultCellFactory(newColSpec);
                rearranger.replace(fac, m_srcImgCol.getStringValue());
                m_data = exec.createColumnRearrangeTable(inData[0], rearranger, exec);
                fac.closeImgOpener();
            } else {
                throw new IllegalArgumentException("The value of the column creation setting is invalid!");
            }

            return new BufferedDataTable[]{m_data};
        } catch (final Exception e) {
            KNIPGateway.log().error("Error when executing Ilastik.");

            throw new IllegalStateException(e);
        } finally {
            cleanUp(tmpDir);
        }
    }

    /**
     * @param newColSpec the column spec
     * @return a cellfactory that reads the result images from ilastik
     */
    private IlastikCellFactory createResultCellFactory(final DataColumnSpec newColSpec) {
        return new IlastikCellFactory(newColSpec);
    }

    /**
     *
     * Read resulting images every channel is a probability map for one labeling
     *
     * @param exec
     *
     * @param exec
     * @param outFiles
     */
    @SuppressWarnings("unchecked")
    private void readResultImages(final ImgPlusCellFactory factory, final DataContainer container) {

        final ScifioImgSource imgOpener = new ScifioImgSource();

        m_outFiles.forEach((final RowKey key, final Pair<String, String> pair) -> {
            try {
                DataCell[] cells = new DataCell[1];

                ImgPlus<RealType> img = imgOpener.getImg(pair.getFirst(), 0);
                img.setSource(pair.getSecond());
                img.setName(key + "_result");
                cells[0] = factory.createCell(img);
                container.addRowToTable(new DefaultRow(key, cells));
            } catch (Exception e) {
                imgOpener.close();
                throw new IllegalStateException(
                        "Can't read image in Ilastik Headless Node at RowId: " + key + " : " + e, e);
            }
        });
        imgOpener.close();
    }

    /**
     * Delete temporary directory
     *
     * @param tmpDir
     * @throws IOException
     */
    private void cleanUp(final File tmpDir) throws IOException {
        //         delete tmp directory
        try {
            FileUtils.forceDelete(tmpDir);
        } catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param exec
     * @throws IOException
     * @throws InterruptedException
     * @throws CanceledExecutionException
     * @throws URISyntaxException
     */
    private void runIlastik(final String tmpDirPath, final List<String> inFiles, final ExecutionContext exec)
            throws IOException, InterruptedException {

        // get path of ilastik
        final String path = IlastikPreferencePage.getPath();

        String outpath;
        try {
            outpath = FileUtil.resolveToPath(FileUtil.toURL(m_pathToIlastikProjectFileModel.getStringValue()))
                    .toAbsolutePath().toString();
        } catch (InvalidPathException | URISyntaxException e) {
            throw new IllegalArgumentException("The Path to the project file could not be resolved: " + e, e);
        }
        if (outpath == null) {
            throw new IllegalArgumentException("The Path to the project file could not be resolved.");
        }

        // DO NOT TOUCH THIS ORDER!
        inFiles.add(0, path);
        inFiles.add(1, "--headless");
        inFiles.add(2, "--project=".concat(outpath));
        inFiles.add(3, "--output_format=tif");
        inFiles.add(4, "--output_filename_format=".concat(tmpDirPath).concat("{nickname}_result"));

        KNIPGateway.log().debug("Executing ilastik with " + String.join(", ", inFiles));

        // build process with project and images
        ProcessBuilder pB = new ProcessBuilder(inFiles);

        // limit cpu + memory usage
        final Map<String, String> env = pB.environment();
        env.put("LAZYFLOW_THREADS", String.valueOf(m_ilastikThreadCount.getIntValue()));
        env.put("LAZYFLOW_TOTAL_RAM_MB", String.valueOf(m_ilastikMaxMemory.getIntValue()));

        // run ilastik
        Process p = pB.start();

        // write ilastik output to knime console
        redirectToKnimeConsole(p.getInputStream(), DirectedLogServiceFactory.debug());
        redirectToKnimeConsole(p.getErrorStream(), DirectedLogServiceFactory.error());

        try {
            while(! p.waitFor(500, TimeUnit.MILLISECONDS)) {
                exec.checkCanceled();
            }
        } catch (CanceledExecutionException cee) {
           KNIPGateway.log().error("Execution canceled, closing Ilastik now.");
           p.destroy();
        }

        // 0 indicates successful execution
        if (p.exitValue() != 0) {
            throw new IllegalStateException("Execution of ilastik was not successful.");
        }
    }

    /**
     * Write stream to knime console
     *
     * @param in input stream
     * @param logService
     * @throws IOException
     */
    static void redirectToKnimeConsole(final InputStream in, final DirectedLogService defaultLogger) {

        Thread t = new Thread() {
            @Override
            public void run() {

                String line;

                try (BufferedReader bis = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()))) {
                    while ((line = bis.readLine()) != null) {
                        if (line.contains("WARNING")) {
                            KNIPGateway.log().warn(line);
                        } else {
                            defaultLogger.log(line);
                        }
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException("Could not read ilastik output", ioe);
                }
            }
        };

        t.setUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler(KNIPGateway.log()));
        t.start();
    }

    /**
     *
     * @param inSpec
     * @return
     * @throws InvalidSettingsException
     */
    private int getImgColIdx(final DataTableSpec inSpec) throws InvalidSettingsException {
        // check settings for the original image, the full picture of
        // the cells.
        int imgColIdx = inSpec.findColumnIndex(m_srcImgCol.getStringValue());

        if (imgColIdx == -1) {
            if ((imgColIdx = NodeUtils.autoOptionalColumnSelection(inSpec, m_srcImgCol, ImgPlusValue.class)) >= 0) {
                setWarningMessage("Auto-configure Image Column: " + m_srcImgCol.getStringValue());
            } else {
                throw new InvalidSettingsException("No column selected!");
            }
        }
        return imgColIdx;
    }

    /**
     * @return SettingsModelString for path to ilastik project file
     */
    public static SettingsModelString createPathToIlastikProjectFileModel() {
        return new SettingsModelString("path_to_ilastik_project_file", "");
    }

    /**
     * @return SettingsModelString for max amount of memory (mb) for ilastik
     */
    public static SettingsModelInteger createIlastikMaxMemoryModel() {
        return new SettingsModelInteger("ilastik_max_memory", (int) (Runtime.getRuntime().maxMemory() / 1024L / 1024L));
    }

    /**
    * @return SettingsModelString for ilastik thread count
    */
   public static SettingsModelIntegerBounded createIlastikThreadCountModel() {
       return new SettingsModelIntegerBounded("ilastik_thread_count", KNIMEConstants.GLOBAL_THREAD_POOL.getMaxThreads(), 1, Integer.MAX_VALUE);
   }

   /**
   * @return SettingsModelString for source image column.
   */
  public static SettingsModelString createImgColModel() {
      return new SettingsModelString("src_image", "");
  }

    /**
     * @return {@link SettingsModelString} for the column creation mode.
     */
    public static SettingsModelString createColCreationModeModel() {
        return new SettingsModelString("colCreationMode", ColCreationModes.NEW_TABLE);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_pathToIlastikProjectFileModel.saveSettingsTo(settings);
        m_srcImgCol.saveSettingsTo(settings);
        m_colCreationModeModel.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikProjectFileModel.validateSettings(settings);
        m_srcImgCol.validateSettings(settings);

        try {
            m_ilastikMaxMemory.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            LOGGER.warn("Problems occurred loading the settings " + m_ilastikMaxMemory.toString() + ": "
                    + e.getLocalizedMessage());
            setWarningMessage("Problems occurred while loading settings.");
        }

        try {
            m_ilastikThreadCount.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            LOGGER.warn("Problems occurred loading the settings " + m_ilastikThreadCount.toString() + ": "
                    + e.getLocalizedMessage());
            setWarningMessage("Problems occurred while loading settings.");
        }


        try {
            m_colCreationModeModel.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            if (("String for key \"colCreationMode\" not found.").equals(e.getMessage())) {
                // Ignore missing colCreationMode settings model for backwards compatibility.
            } else {
                throw e;
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikProjectFileModel.loadSettingsFrom(settings);
        m_srcImgCol.loadSettingsFrom(settings);

        try {
            m_ilastikMaxMemory.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            LOGGER.warn("Problems occurred loading the settings " + m_ilastikMaxMemory.toString() + ": "
                    + e.getLocalizedMessage());
            setWarningMessage("Problems occurred while loading settings.");
        }

        try {
            m_ilastikThreadCount.validateSettings(settings);
        } catch (InvalidSettingsException e) {
            LOGGER.warn("Problems occurred loading the settings " + m_ilastikThreadCount.toString() + ": "
                    + e.getLocalizedMessage());
            setWarningMessage("Problems occurred while loading settings.");
        }

        try {
            m_colCreationModeModel.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            if (("String for key \"colCreationMode\" not found.").equals(e.getMessage())) {
                // Ignore missing colCreationMode settings model for backwards compatibility.
            } else {
                throw e;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        m_data = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BufferedDataTable[] getInternalTables() {
        return new BufferedDataTable[]{m_data};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInternalTables(final BufferedDataTable[] tables) {
        m_data = tables[0];

    }

    class IlastikCellFactory extends SingleCellFactory {
        private ScifioImgSource imgOpener;

        /**
         * @param newColSpec
         */
        public IlastikCellFactory(final DataColumnSpec newColSpec) {
            super(newColSpec);
            imgOpener = new ScifioImgSource();
        }

        @SuppressWarnings("unchecked")
        @Override
        // Get the scores weighted by the exploration factor
        public DataCell getCell(final DataRow row) {
            RowKey key = row.getKey();
            Pair<String, String> names = m_outFiles.get(key);
            DataCell cell;
            try {
                String outfile = names.getFirst();
                String source = names.getSecond();
                final ImgPlus<T> img = (ImgPlus<T>)imgOpener.getImg(outfile, 0);
                img.setSource(source);
                img.setName(key + "_result");
                cell = m_imgPlusCellFactory.createCell(img);
            } catch (Exception e) {
                cell = new MissingCell("Error during execution: " + e);
            }
            return cell;
        }

        public void closeImgOpener() {
            imgOpener.close();
        }
    }


    interface DirectedLogService {
        public void log(Object arg0);
        public void log(Object arg0, Throwable arg1);
    }

    static class ErrorLogService implements DirectedLogService {
        @Override
        public void log(final Object arg0) {
            KNIPGateway.log().error(arg0);
        }

        @Override
        public void log(final Object arg0, final Throwable arg1) {
            KNIPGateway.log().error(arg0);
        }

    }

    static class DebugLogService implements DirectedLogService {
        @Override
        public void log(final Object arg0) {
            KNIPGateway.log().debug(arg0);
        }

        @Override
        public void log(final Object arg0, final Throwable arg1) {
            KNIPGateway.log().debug(arg0);
        }

    }

    static class DirectedLogServiceFactory {
        private static ErrorLogService m_errorLogService;

        private static DebugLogService m_debugLogService;

        public static ErrorLogService error() {
            if (m_errorLogService == null) {
                m_errorLogService = new ErrorLogService();
            }
            return m_errorLogService;
        }

        public static DebugLogService debug() {
            if (m_debugLogService == null) {
                m_debugLogService = new DebugLogService();
            }
            return m_debugLogService;
        }
    }

}
