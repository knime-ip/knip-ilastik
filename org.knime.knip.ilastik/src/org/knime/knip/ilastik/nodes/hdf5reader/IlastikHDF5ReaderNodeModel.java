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
 * Created on Jul 16, 2014 by andreasgraumann
 */
package org.knime.knip.ilastik.nodes.hdf5reader;

import java.io.File;
import java.io.IOException;

import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.knip.base.data.img.ImgPlusCell;

import ch.systemsx.cisd.hdf5.HDF5CompoundDataMap;
import ch.systemsx.cisd.hdf5.HDF5CompoundMemberInformation;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5CompoundReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;

/**
 *
 * @author Andreas Graumann, University of Konstanz
 * @author Christian Dietz, University of Konstanz
 */
public class IlastikHDF5ReaderNodeModel<T extends NativeType<T> & RealType<T>> extends NodeModel {

    SettingsModelString m_fileChooser = createFileChooserModel();

    // Constants for reading
    final static String[] KNIME_DIMENSION_ORDER = new String[]{"X", "Y", "Channel", "Z", "Time"};

    final static String TYPE_ATTRIBUTE = "type";

    final static String AXIS_ATTRIBUTE = "axistags";

    final static String RAW_IMAGE_PATH = "images/raw";

    final static String IMG_FOLDER_NAME = "images";

    final static String TABLE_NAME = "table";

    // Constants for output

    final static String IMAGE_COL_PREFIX = "Raw Image";

    final static String LABELING_COL_PREFIX = "Labeling";

    final static String SOURCE_NAME = "Ilastik";

    enum DatasetType {
        IMAGE("image"), LABELING("labeling");

        private String identifier;

        DatasetType(final String _identifier) {
            this.identifier = _identifier;
        }

        public boolean isType(final String other) {
            return identifier.equalsIgnoreCase(other);
        }
    }

    static final SettingsModelString createFileChooserModel() {
        return new SettingsModelString("file_chooser", "");
    }

    /**
     * @param nrInDataPorts
     * @param nrOutDataPorts
     */
    protected IlastikHDF5ReaderNodeModel(final int nrInDataPorts, final int nrOutDataPorts) {
        super(nrInDataPorts, nrOutDataPorts);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        if (new File(m_fileChooser.getStringValue()).exists()) {
            return new DataTableSpec[]{createFeatureOutputSpec(), createRawImageOutputSpec()};
        } else {
            return null;
        }
    }

    /**
     * Creates Data table spec for output table
     *
     * @return
     */
    private DataTableSpec createFeatureOutputSpec() {

        // get HDF5 Reader
        final IHDF5Reader reader = HDF5Factory.openForReading(m_fileChooser.getStringValue());

        final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);
        final DataColumnSpec[] specs = new DataColumnSpec[dataSetInfo.length];

        int i = 0;
        for (final HDF5CompoundMemberInformation memberInfo : dataSetInfo) {
            specs[i] = new DataColumnSpecCreator(memberInfo.getName(), typeMatcher(memberInfo.getType())).createSpec();
            i++;
        }

        return new DataTableSpec(specs);

    }

    /**
     * Creates data table spec for the complete raw image
     *
     * @return Data table spec for complete raw image
     */
    private DataTableSpec createRawImageOutputSpec() {
        return new DataTableSpec(new DataColumnSpecCreator(IMAGE_COL_PREFIX, ImgPlusCell.TYPE).createSpec());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {

        // container for output data
        BufferedDataContainer featureContainer = null;
        BufferedDataContainer rawImageContainer = null;

        // check if file is valid
        if (new File(m_fileChooser.getStringValue()).exists()) {
            featureContainer = exec.createDataContainer(createFeatureOutputSpec());
            rawImageContainer = exec.createDataContainer(createRawImageOutputSpec());
        } else {
            throw new IllegalArgumentException("Path is not a .h5 file");
        }

        // open hdf5 file
        final IHDF5Reader reader = HDF5Factory.openForReading(m_fileChooser.getStringValue());
        final IHDF5CompoundReader compound = reader.compound();
        final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);

        // store the list
        final HDF5CompoundDataMap[] map = compound.readArray(TABLE_NAME, HDF5CompoundDataMap.class);

        // create row
        for (int i = 0; i < map.length; i++) {

            // create array with data cells
            final DataCell[] cells = new DataCell[map[i].size()];

            for (int j = 0; j < dataSetInfo.length; j++) {

                HDF5CompoundMemberInformation info = dataSetInfo[j];
                // get cell type
                final Class<?> cellType = clazzMatcher(info.getType());

                // create data cell;
                cells[j] = createDataCell(info.getType(), map[i].get(info.getName()));
            }
            // add row to table
            featureContainer.addRowToTable(new DefaultRow("Row" + i, cells));
        }

        // close feature container
        featureContainer.close();
        rawImageContainer.close();

        return new BufferedDataTable[]{featureContainer.getTable(), rawImageContainer.getTable()};
    }

    /**
     *
     * @param cellType
     * @param value
     * @return
     */
    private DataCell createDataCell(final HDF5DataTypeInformation cellType, Object value) {
        switch (cellType.getDataClass()) {
            case FLOAT:
                if (value.getClass().getName().contains("Float")) {
                    value = ((Float)value).doubleValue();
                }
                return new DoubleCell((Double)value);
            case INTEGER:
                if (value.getClass().getName().contains("Long")) {
                    value = ((Long)value).intValue();
                }
                return new IntCell((Integer)value);
            case STRING:
                return new StringCell((String)value);
            default:
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
        }
        return null;
    }

    /**
     *
     * @param type
     * @return
     */
    private DataType typeMatcher(final HDF5DataTypeInformation type) {
        switch (type.getDataClass()) {
            case BITFIELD:
                return BooleanCell.TYPE;
            case FLOAT:
                return DoubleCell.TYPE;
            case INTEGER:
                return IntCell.TYPE;
            case STRING:
                return StringCell.TYPE;
            default:
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
        }
        return null;
    }

    /**
     * Resolve class type of hdf5 type
     *
     * @param type hdf5 information type
     * @return class type
     */
    private Class<?> clazzMatcher(final HDF5DataTypeInformation type) {
        switch (type.getDataClass()) {
            case BITFIELD:
                return Boolean.class;
            case FLOAT:
                if (type.getElementSize() == 4) {
                    return Float.class;
                }
                if (type.getElementSize() == 8) {
                    return Double.class;
                }
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
            case INTEGER:
                if (type.getElementSize() == 1) {
                    return Byte.class;
                }
                if (type.getElementSize() == 2) {
                    return Short.class;
                }
                if (type.getElementSize() == 4) {
                    return Integer.class;
                }
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
            case STRING:
                return String.class;
            default:
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
        }
        return null;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_fileChooser.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileChooser.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fileChooser.loadSettingsFrom(settings);
    }

    @Override
    protected void reset() {
        // Nothing to do here
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // nothing to do here
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // nothing to do here
    }
}
