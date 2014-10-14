/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2014
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
 * Created on Oct 2, 2014 by andreasgraumann
 */
package org.knime.knip.ilastik.nodes.headless;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.knip.base.data.img.ImgPlusValue;
import org.knime.knip.io.nodes.imgwriter.ImgWriter;

/**
 *
 * @author Andreas Graumann, University of Konstanz
 */
public class IlastikHeadlessNodeModel extends NodeModel {

    /**
     * Path to ilastik installation
     */
    private SettingsModelString m_pathToIlastikInstallationModel = createPathToIlastikInstallationModel();

    /**
     * Path to ilastik project file
     */
    private SettingsModelString m_pathToIlastikProjectFileModel = createPathToIlastikProjectFileModel();

    /**
     * @param nrInDataPorts
     * @param nrOutDataPorts
     */
    protected IlastikHeadlessNodeModel(final int nrInDataPorts, final int nrOutDataPorts) {
        super(nrInDataPorts, nrOutDataPorts);
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {

        return new DataTableSpec[]{new DataTableSpec(createImgSpec())};
    }

    /**
     * @return
     */
    private DataColumnSpec createImgSpec() {
        return new DataColumnSpecCreator("Result Image Image", ImgPlusCell.TYPE).createSpec();
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {

        // create tmp directory
        String tmpDirPath = System.getProperty("java.io.tmpdir") + "/ilastik" + System.currentTimeMillis() + "/";
        File tmpDir = new File(tmpDirPath);
        tmpDir.mkdir();

        List<String> files = new ArrayList<String>();

        // copy images to tmp directory
        final CloseableRowIterator it = inData[0].iterator();

        // iterate over all input images and copy them to the tmp directory
        while (it.hasNext()) {
            DataRow next = it.next();
            final ImgPlusValue<?> img = (ImgPlusValue<?>)next.getCell(0);

            final int[] map = new int[img.getDimensions().length];

            for (int i = 0; i < map.length; i++) {
                map[i] = i;
            }

            ImgWriter iW = new ImgWriter();
            files.add(tmpDirPath+next.getKey().getString()+".tif");
            iW.writeImage(img.getImgPlus(), tmpDirPath + next.getKey().getString() + ".tif",
                          "Tagged Image File Format (tif)", "Uncompressed", map);
        }

        // run ilastik and process images
        runIlastik(tmpDirPath, files);

        // make Output from resulting images

        // delte tmp directory
        tmpDir.delete();

        // return result images (same spec as input??)
        return inData;
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    private void runIlastik(final String tmpDirPath, final List<String> files) throws IOException, InterruptedException {

        String macExtension = "";

        // which OS are we working on?
        String os = System.getProperty("os.name");

        // On Mac OS X we must call the program within the app to be able to add arguments
        if (os.contains("OS")) {
            macExtension = "/Contents/MacOS/ilastik";
        }

        // DO NOT TOUCH THIS ORDER!
        inFiles.add(0, m_pathToIlastikInstallationModel.getStringValue().concat(macExtension));
        inFiles.add(1, "--headless");
        inFiles.add(2, "--project=".concat(m_pathToIlastikProjectFileModel.getStringValue()));
        inFiles.add(3, "--output_format=tif");
        inFiles.add(4, "--output_filename_format=".concat(tmpDirPath).concat("{nickname}_result"));

        // build process with project and images
        ProcessBuilder pB = new ProcessBuilder(files);
//        ProcessBuilder pB =
//                new ProcessBuilder(m_pathToIlastikInstallationModel.getStringValue().concat(macExtension),
//                        "--headless", "--project=".concat(m_pathToIlastikProjectFileModel.getStringValue()),
//                        "--output_format=tiff", "--output_filename_format=/Users/andreasgraumann/tmp/results.tiff",
//                        tmpDirPath+"tmp.tif.tif");

        // run ilastiks
        Process p = pB.start();
        p.waitFor();

        // copy ilastik output to system.out
        copy(p.getInputStream(), System.out);

        // 0 indicates successful execution
        if (p.waitFor() == 0) {
            return true;
        } else {
            return false;
        }
    }

    // stream copy method
    static void copy(final InputStream in, final OutputStream out) throws IOException {
        while (true) {
            int c = in.read();
            if (c == -1) {
                break;
            }
            out.write((char)c);
        }
    }

    /**
     * @return
     */
    public static SettingsModelString createPathToIlastikProjectFileModel() {
        return new SettingsModelString("path_to_ilastik_project_file", "");
    }

    /**
     * @return
     */
    public static SettingsModelString createPathToIlastikInstallationModel() {
        return new SettingsModelString("path_to_ilastik_installation", "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_pathToIlastikInstallationModel.saveSettingsTo(settings);
        m_pathToIlastikProjectFileModel.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikInstallationModel.validateSettings(settings);
        m_pathToIlastikProjectFileModel.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_pathToIlastikInstallationModel.loadSettingsFrom(settings);
        m_pathToIlastikProjectFileModel.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // nothing to do here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // nothing to do here
    }

}
