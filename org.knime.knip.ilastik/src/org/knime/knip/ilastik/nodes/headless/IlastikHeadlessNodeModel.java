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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.util.Pair;
import org.knime.knip.base.data.img.ImgPlusCell;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.base.data.img.ImgPlusValue;
import org.knime.knip.base.node.NodeUtils;
import org.knime.knip.core.KNIPGateway;
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

    public static final String[] COL_CREATION_MODES = new String[]{"New Table", "Append", "Replace"};

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
     * data table for table cell view
     */
    private BufferedDataTable m_data;

    private Map<RowKey, Pair<String, String>> m_outFiles;

    private ImgPlusCellFactory m_imgPlusCellFactory;

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
        DataTableSpec outSpec = null;
        String colCreationMode = m_colCreationModeModel.getStringValue();

        if (colCreationMode.equals(COL_CREATION_MODES[0])) { // new table
            outSpec = createImgSpec();
        } else if (colCreationMode.equals(COL_CREATION_MODES[1])) { // Append
            outSpec = createAppendRearanger(inSpecs[0]).createSpec();
        } else if (colCreationMode.equals(COL_CREATION_MODES[2])) { // Replace
            outSpec = createReplaceRearanger(inSpecs[0]).createSpec();
        } else {
            throw new IllegalArgumentException("The value of the column creation setting is invalid!");
        }

        return new DataTableSpec[]{outSpec};
    }

    /**
     * @return
     */
    private DataTableSpec createImgSpec() {
        return new DataTableSpec(new DataColumnSpecCreator("Result", ImgPlusCell.TYPE).createSpec());
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
        final List<String> files = new ArrayList<String>();

        final ImgWriter2 iW = new ImgWriter2();

        m_outFiles = new LinkedHashMap<>();
        m_imgPlusCellFactory = new ImgPlusCellFactory(exec);

        // iterate over all input images and copy them to the tmp directory
        for (DataRow row : inData[0]) {

            final DataCell cell = row.getCell(idx);

            if (cell.isMissing()) {
                KNIPGateway.log().warn("Ignoring missing cell in row " + row.getKey() + "!");
                continue;
            }

            // get next image
            final ImgPlusValue<?> imgvalue = (ImgPlusValue<?>)cell;

            final String fileName = tmpDirPath + row.getKey().getString() + ".tif";

            // store in-image name in list
            files.add(fileName);

            // store out-image name in iterable
            // store in map
            m_outFiles.put(row.getKey(), new Pair<>(fileName, imgvalue.getImgPlus().getSource()));

            // map for dimensions
            final int[] map = new int[imgvalue.getDimensions().length];
            for (int i = 0; i < map.length; i++) {
                map[i] = i;
            }

            // Image Writer

            // write image to temp folder as input for ilastik
            iW.writeImage(imgvalue.getImgPlus(), fileName, "TIFF (tif)", "Uncompressed", map);
        }

        try {
            // run ilastik and process images
            runIlastik(tmpDirPath, files);

            String colCreationMode = m_colCreationModeModel.getStringValue();

            if (colCreationMode.equals(COL_CREATION_MODES[0])) { // new table

                BufferedDataContainer container = null;
                container = exec.createDataContainer(createImgSpec());
                readResultImages(new ImgPlusCellFactory(exec), container);
                container.close();
                m_data = container.getTable();

            } else if (colCreationMode.equals(COL_CREATION_MODES[1])) { // Append
                m_data = exec.createColumnRearrangeTable(inData[0], createAppendRearanger(inData[0].getSpec()), exec);
            } else if (colCreationMode.equals(COL_CREATION_MODES[2])) { // Replace
                m_data = exec.createColumnRearrangeTable(inData[0], createReplaceRearanger(inData[0].getSpec()), exec);
            } else {
                throw new IllegalArgumentException("The value of the column creation setting is invalid!");
            }
            cleanUp(tmpDir);

            return new BufferedDataTable[]{m_data};
        } catch (final Exception e) {
            KNIPGateway.log()
                    .error("Error when executing Ilastik. Please check the dimensionality of the input images.");
            cleanUp(tmpDir);
            throw new RuntimeException(e);
        }
    }

    /**
     * @param listRowKey
     * @param imgPlusCellFactory
     * @return
     */
    private ColumnRearranger createReplaceRearanger(final DataTableSpec inSpec) {
        final ColumnRearranger rearranger = new ColumnRearranger(inSpec);
        final DataColumnSpec newColSpec = inSpec.getColumnSpec(m_srcImgCol.getStringValue());

        // utility object that performs the calculation
        rearranger.replace(new SingleCellFactory(newColSpec) {

            ScifioImgSource imgOpener = new ScifioImgSource();

            @SuppressWarnings("unchecked")
            @Override
            // Get the scores weighted by the exploration factor
            public DataCell getCell(final DataRow row) {
                RowKey key = row.getKey();
                Pair<String, String> name = m_outFiles.get(key);
                String outfile = name.getFirst();
                String source = name.getSecond();

                DataCell cell;
                try {
                    ImgPlus<T> img = (ImgPlus<T>)imgOpener.getImg(outfile, 0);
                    img.setSource(source);
                    cell = m_imgPlusCellFactory.createCell(img);
                } catch (Exception e) {
                    cell = new MissingCell(outfile);
                }
                return cell;
            }
        }, m_srcImgCol.getStringValue());
        return rearranger;
    }

    /**
     * @return
     */
    private ColumnRearranger createAppendRearanger(final DataTableSpec inSpec) {
        final ColumnRearranger rearranger = new ColumnRearranger(inSpec);
        final DataColumnSpec newColSpec = new DataColumnSpecCreator("Result", ImgPlusCell.TYPE).createSpec();

        // utility object that performs the calculation
        rearranger.append(new SingleCellFactory(newColSpec) {

            ScifioImgSource imgOpener = new ScifioImgSource();

            @SuppressWarnings("unchecked")
            @Override
            // Get the scores weighted by the exploration factor
            public DataCell getCell(final DataRow row) {
                RowKey key = row.getKey();
                Pair<String, String> names = m_outFiles.get(key);
                String outfile = names.getFirst();
                String source = names.getSecond();
                DataCell cell;
                try {
                    ImgPlus<T> img = (ImgPlus<T>)imgOpener.getImg(outfile, 0);
                    img.setSource(source);
                    cell = m_imgPlusCellFactory.createCell(img);
                } catch (Exception e) {
                    cell = new MissingCell(outfile);
                }
                return cell;
            }
        });
        return rearranger;
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
                cells[0] = factory.createCell(img);
                container.addRowToTable(new DefaultRow(key, cells));
            } catch (Exception e) {
                imgOpener.close();
                throw new IllegalStateException("Can't read image in Ilastik Headless Node at RowId" + key);
            }
        });
        imgOpener.close();
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
        // inFiles.add(0, "/bin/bash");
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
            KNIPGateway.log().info(line);
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
     * @return SettingsModelString for source image column.
     */
    public static SettingsModelString createImgColModel() {
        return new SettingsModelString("src_image", "");
    }

    /**
     * @return {@link SettingsModelString} for the column creation mode.
     */
    public static SettingsModelString createColCreationModeModel() {
        return new SettingsModelString("colCreationMode", COL_CREATION_MODES[0]);

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
        m_colCreationModeModel.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikProjectFileModel.loadSettingsFrom(settings);
        m_srcImgCol.loadSettingsFrom(settings);
        m_colCreationModeModel.loadSettingsFrom(settings);
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

}
