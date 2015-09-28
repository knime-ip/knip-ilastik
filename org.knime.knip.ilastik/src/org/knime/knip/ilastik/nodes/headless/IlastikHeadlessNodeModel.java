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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CellFactory;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.knip.base.data.img.ImgPlusCell;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.base.data.img.ImgPlusValue;
import org.knime.knip.base.node.NodeUtils;
import org.knime.knip.ilastik.nodes.IlastikPreferencePage;
import org.knime.knip.io.ScifioImgSource;
import org.knime.knip.io.nodes.imgwriter2.ImgWriter2;

import net.imagej.ImgPlus;
import net.imglib2.type.numeric.RealType;

/**
 *
 * Headless execution of an Ilastik project in a KNIME node.
 *
 * @author Andreas Graumann, University of Konstanz
 * @param <T>
 */
public class IlastikHeadlessNodeModel<T extends RealType<T>> extends NodeModel implements BufferedDataTableHolder {

    /**
     * Path to ilastik project file
     */
    private SettingsModelString m_pathToIlastikProjectFileModel = createPathToIlastikProjectFileModel();

    /**
     * Source image column
     */
    private final SettingsModelString m_srcImgCol = createImgColModel();

    /**
     * data table for table cell view
     */
    private BufferedDataTable m_data;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(IlastikHeadlessNodeModel.class);

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

        final ColumnRearranger rearranger = new ColumnRearranger(inSpecs[0]);
        rearranger.append(createCellFactory(inSpecs[0], null));

        return new DataTableSpec[]{rearranger.createSpec()};
    }

    /**
     *
     * @param inSpec
     * @return
     * @throws InvalidSettingsException
     */
    private CellFactory createCellFactory(final DataTableSpec inSpec, final HashMap<RowKey, DataCell[]> dataCells)
            throws InvalidSettingsException {

        return new CellFactory() {

            HashMap<RowKey, DataCell[]> m_dataCells = dataCells;

            @Override
            public DataCell[] getCells(final DataRow row) {
                return m_dataCells.get(row.getKey());
            }

            @Override
            public DataColumnSpec[] getColumnSpecs() {
                return new DataColumnSpec[]{createImgSpec()};
            }

            @Override
            public void setProgress(final int curRowNr, final int rowCount, final RowKey lastKey,
                                    final ExecutionMonitor exec) {
                exec.setProgress((double)curRowNr / rowCount);
            }
        };
    }

    /**
     * @return
     */
    private DataColumnSpec createImgSpec() {
        return new DataColumnSpecCreator("Resulting Image", ImgPlusCell.TYPE).createSpec();
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {

        m_data = inData[0];

        // get index of image column
        int idx = getImgColIdx(inData[0].getSpec());

        // create tmp directory
        String tmpDirPath = System.getProperty("java.io.tmpdir") + "/ilastik" + System.currentTimeMillis() + "/";
        File tmpDir = new File(tmpDirPath);
        tmpDir.mkdir();

        // list to store all input file names
        List<String> files = new ArrayList<String>();

        // copy images to tmp directory
        final CloseableRowIterator it = inData[0].iterator();

        HashMap<String, RowKey> mapImgRowKey = new HashMap<String, RowKey>();

        // iterate over all input images and copy them to the tmp directory
        while (it.hasNext()) {
            DataRow next = it.next();

            // get next image
            final ImgPlusValue<?> img = (ImgPlusValue<?>)next.getCell(idx);

            String inFile = tmpDirPath + next.getKey().getString() + ".tif";

            // store in-image name in list
            files.add(inFile);

            String outFile = tmpDirPath + next.getKey().getString();
            // store out-image name in iterable
            // store in map
            mapImgRowKey.put(outFile, next.getKey());

            // map for dimensions
            final int[] map = new int[img.getDimensions().length];
            for (int i = 0; i < map.length; i++) {
                map[i] = i;
            }

            // Image Writer
            ImgWriter2 iW = new ImgWriter2();

            // write image to temp folder as input for ilastik
            iW.writeImage(img.getImgPlus(), tmpDirPath + next.getKey().getString() + ".tif", "TIFF (tif)",
                          "Uncompressed", map);
        }

        try {
            // run ilastik and process images
            runIlastik(tmpDirPath, files);

            HashMap<RowKey, DataCell[]> dataCells = readResultImages(exec, mapImgRowKey);

            final ColumnRearranger rearranger = new ColumnRearranger(inData[0].getDataTableSpec());
            rearranger.append(createCellFactory(inData[0].getDataTableSpec(), dataCells));

            final BufferedDataTable[] res =
                    new BufferedDataTable[]{exec.createColumnRearrangeTable(inData[0], rearranger, exec)};
            m_data = res[0];

            cleanUp(tmpDir);
            return res;
        } catch (Exception e) {
            LOGGER.error("Error when executing Ilastik. Please check the dimension of the input images.");
            cleanUp(tmpDir);
            return inData;
        }
    }

    /**
     *
     * Read resulting images every channel is a probability map for one labeling
     *
     * @param exec
     *
     * @param exec
     * @param mapImgRowKey
     * @return
     */
    @SuppressWarnings("unchecked")
    private HashMap<RowKey, DataCell[]> readResultImages(final ExecutionContext exec,
                                                         final HashMap<String, RowKey> mapImgRowKey) {

        HashMap<RowKey, DataCell[]> dataCells = new HashMap<RowKey, DataCell[]>();
        final ScifioImgSource imgOpener = new ScifioImgSource();

        for (Entry<String, RowKey> e : mapImgRowKey.entrySet()) {
            try {
                ImgPlus<T> img = (ImgPlus<T>)imgOpener.getImg(e.getKey(), 0);
                DataCell[] cells = new DataCell[1];
                cells[0] = new ImgPlusCellFactory(exec).createCell(img);
                dataCells.put(e.getValue(), cells);
            } catch (Exception p) {
                throw new IllegalStateException("Can't read image " + e.getKey());
            }
        }
        imgOpener.close();

        return dataCells;
    }

    /**
     * Delete temporary directory
     *
     * @param tmpDir
     */
    private void cleanUp(final File tmpDir) {
        // delete tmp directory
        tmpDir.delete();
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean runIlastik(final String tmpDirPath, final List<String> inFiles)
            throws IOException, InterruptedException {

        // get path of ilastik
        final String path = IlastikPreferencePage.getPath();

        // DO NOT TOUCH THIS ORDER!
        inFiles.add(0, path);
        inFiles.add(1, "--headless");
        inFiles.add(2, "--project=".concat(m_pathToIlastikProjectFileModel.getStringValue()));
        inFiles.add(3, "--output_format=tif");
        inFiles.add(4, "--output_filename_format=".concat(tmpDirPath).concat("{nickname}_result"));

        // build process with project and images
        ProcessBuilder pB = new ProcessBuilder(inFiles);

        // run ilastik
        Process p = pB.start();

        // write ilastik output to knime console
        writeToKnimeConsole(p.getInputStream());

        // 0 indicates successful execution
        if (p.waitFor() == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Write stream to knime console
     *
     * @param in input stream
     * @throws IOException
     */
    static void writeToKnimeConsole(final InputStream in) throws IOException {
        BufferedReader bis = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));
        String line;
        while ((line = bis.readLine()) != null) {
            LOGGER.info(line);
        }
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
     *
     * @return SettingsModelString for source image column
     */
    public static SettingsModelString createImgColModel() {
        return new SettingsModelString("src_image", "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_pathToIlastikProjectFileModel.saveSettingsTo(settings);
        m_srcImgCol.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikProjectFileModel.validateSettings(settings);
        m_srcImgCol.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikProjectFileModel.loadSettingsFrom(settings);
        m_srcImgCol.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothin to do here
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

}
