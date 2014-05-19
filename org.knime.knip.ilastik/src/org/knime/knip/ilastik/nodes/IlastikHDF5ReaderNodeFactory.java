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
 *  propagated with or for interoperation with KNIME. The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Apr 11, 2012 (hornm): created
 */
package org.knime.knip.ilastik.nodes;

import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.labeling.Labeling;
import net.imglib2.labeling.LabelingType;
import net.imglib2.labeling.NativeImgLabeling;
import net.imglib2.meta.Axes;
import net.imglib2.meta.CalibratedAxis;
import net.imglib2.meta.CalibratedSpace;
import net.imglib2.meta.DefaultCalibratedSpace;
import net.imglib2.meta.DefaultNamed;
import net.imglib2.meta.DefaultSourced;
import net.imglib2.meta.ImgPlus;
import net.imglib2.meta.Named;
import net.imglib2.meta.Sourced;
import net.imglib2.meta.axis.DefaultLinearAxis;
import net.imglib2.meta.axis.LinearAxis;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
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
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NodeView;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.knip.base.data.img.ImgPlusCell;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.base.data.labeling.LabelingCell;
import org.knime.knip.base.data.labeling.LabelingCellFactory;
import org.knime.knip.core.data.img.DefaultImageMetadata;
import org.knime.knip.core.data.img.DefaultImgMetadata;
import org.knime.knip.core.data.img.DefaultLabelingMetadata;

import ch.systemsx.cisd.base.mdarray.MDAbstractArray;
import ch.systemsx.cisd.hdf5.HDF5CompoundDataMap;
import ch.systemsx.cisd.hdf5.HDF5CompoundMemberInformation;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5DataTypeInformation;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5CompoundReader;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import ch.systemsx.cisd.hdf5.IHDF5StringReader;

/**
 * Reader to reader files which were exported from Ilastik to be read by KNIME
 *
 * @author Christian Dietz, University of Konstanz
 *
 * @param <T>
 */
public class IlastikHDF5ReaderNodeFactory<T extends NativeType<T> & RealType<T>> extends NodeFactory<NodeModel> {

    // Constants for reading
    final static String[] KNIME_DIMENSION_ORDER = new String[]{"X", "Y", "Channel", "Z", "Time"};

    final static String TYPE_ATTRIBUTE = "type";

    final static String AXIS_ATTRIBUTE = "axistags";

    final static String IMG_FOLDER_NAME = "images";

    final static String TABLE_NAME = "table";

    // Constants for output

    final static String IMAGE_COL_PREFIX = "Image";

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

    static SettingsModelString createPathModel() {
        return new SettingsModelString("path_model", "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeModel createNodeModel() {
        return new NodeModel(0, 2) {

            private SettingsModelString m_pathModel = createPathModel();

            /**
             * {@inheritDoc}
             */
            @Override
            protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
                if (new File(m_pathModel.getStringValue()).exists()) {
                    return new DataTableSpec[]{createFirstOutSpec(), createSecondOutSpec()};
                } else {
                    return null;
                }
            }

            //
            private DataTableSpec createSecondOutSpec() {

                final IHDF5Reader reader = HDF5Factory.openForReading(m_pathModel.getStringValue());

                final String exampleDatasetName = reader.getGroupMembers(IMG_FOLDER_NAME).get(0);
                final List<String> exampleMembers = reader.getGroupMembers(IMG_FOLDER_NAME + "/" + exampleDatasetName);
                final DataColumnSpec[] specs = new DataColumnSpec[exampleMembers.size()];

                int imgIdx = 0;
                int labelingIdx = 0;
                for (int i = 0; i < exampleMembers.size(); i++) {
                    //                    final String type =
                    //                            reader.string().getAttr(IMG_FOLDER_NAME + "/" + exampleDatasetName + "/"
                    //                                                            + exampleMembers.get(i), TYPE_ATTRIBUTE);

                    final String type = "image";

                    if (DatasetType.IMAGE.isType(type)) {
                        specs[i] =
                                new DataColumnSpecCreator(IMAGE_COL_PREFIX + imgIdx++, ImgPlusCell.TYPE).createSpec();
                    } else if (DatasetType.LABELING.isType(type)) {
                        specs[i] =
                                new DataColumnSpecCreator(LABELING_COL_PREFIX + labelingIdx++, LabelingCell.TYPE)
                                        .createSpec();
                    } else {
                        throw new IllegalStateException(
                                "Unknown data type. Since now only Images and Labelings are supported!");
                    }
                }

                return new DataTableSpec(specs);
            }

            private DataTableSpec createFirstOutSpec() {
                final IHDF5Reader reader = HDF5Factory.openForReading(m_pathModel.getStringValue());
                final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);
                final DataColumnSpec[] specs = new DataColumnSpec[dataSetInfo.length];

                int i = 0;
                for (final HDF5CompoundMemberInformation memberInfo : dataSetInfo) {
                    specs[i] =
                            new DataColumnSpecCreator(memberInfo.getName(), typeMatcher(memberInfo.getType()))
                                    .createSpec();
                    i++;
                }

                return new DataTableSpec(specs);
            }

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

            /**
             * {@inheritDoc}
             */
            @SuppressWarnings("unchecked")
            @Override
            protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
                    throws Exception {

                BufferedDataContainer firstContainer = null;
                BufferedDataContainer secondContainer = null;
                final DataTableSpec firstSpec = createFirstOutSpec();
                final DataTableSpec secondSpec = createSecondOutSpec();

                if (new File(m_pathModel.getStringValue()).exists()) {
                    firstContainer = exec.createDataContainer(firstSpec);
                    secondContainer = exec.createDataContainer(secondSpec);
                } else {
                    throw new IllegalArgumentException("Path is not a .h5 file");
                }

                final IHDF5Reader reader = HDF5Factory.openForReading(m_pathModel.getStringValue());
                final IHDF5CompoundReader compound = reader.compound();
                final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);

                // store the list
                final HDF5CompoundDataMap[] map = compound.readArray(TABLE_NAME, HDF5CompoundDataMap.class);

                for (int i = 0; i < map.length; i++) {
                    final HDF5CompoundDataMap currentMap = map[i];
                    final DataCell[] cells = new DataCell[currentMap.size()];

                    int j = 0;
                    for (final HDF5CompoundMemberInformation info : dataSetInfo) {
                        final Class<?> matchedClass = clazzMatcher(info.getType());

                        final Object val = currentMap.get(info.getName());
                        if (matchedClass.equals(Byte.class)) {
                            cells[j] = new IntCell((Byte)val);
                        } else if (matchedClass.equals(Float.class)) {
                            cells[j] = new DoubleCell((Float)val);
                        } else if (matchedClass.equals(Double.class)) {
                            cells[j] = new DoubleCell((Double)val);
                        } else if (matchedClass.equals(Short.class)) {
                            cells[j] = new IntCell((Short)val);
                        } else if (matchedClass.equals(Integer.class)) {
                            cells[j] = new IntCell((Integer)val);
                        } else if (matchedClass.equals(String.class)) {
                            cells[j] = new StringCell((String)val);
                        } else {
                            throw new IllegalArgumentException("Can't read feature table, unknown type");
                        }
                        j++;
                    }

                    firstContainer.addRowToTable(new DefaultRow("Row" + i, cells));
                }
                firstContainer.close();

                // read images
                final LabelingCellFactory labelingFactory = new LabelingCellFactory(exec);
                final ImgPlusCellFactory imageFactory = new ImgPlusCellFactory(exec);

                final IHDF5StringReader stringReader = reader.string();
                final List<String> datasets = reader.getGroupMembers(IMG_FOLDER_NAME);
                for (final String dataset : datasets) {

                    final List<DataCell> cells = new ArrayList<DataCell>();

                    for (final String element : reader.getGroupMembers(IMG_FOLDER_NAME + "/" + dataset)) {
                        final String elementPath = IMG_FOLDER_NAME + "/" + dataset + "/" + element;
                        final HDF5DataSetInformation info = reader.getDataSetInformation(elementPath);

                        final JsonParser jsonParser =
                                Json.createParser(new CharArrayReader(stringReader.getAttr(elementPath, AXIS_ATTRIBUTE)
                                        .toCharArray()));

                        final ArrayList<String> axes = new ArrayList<String>();
                        while (jsonParser.hasNext()) {
                            final Event next = jsonParser.next();
                            switch (next) {
                                case KEY_NAME:
                                    if (jsonParser.getString().equalsIgnoreCase("key")) {
                                        jsonParser.next();
                                        axes.add(jsonParser.getString());
                                    }
                                    break;
                                default:
                            }
                        }

                        // go for some metadata
                        final String type = "image";
                        // TODO when format is fixed stringReader.getAttr(elementPath, TYPE_ATTRIBUTE);

                        // Mapping to map dimensions labels from knime <-> vigra
                        final int[] mappingKNIMEToVigra = createVigraToKnimeMapping(axes);
                        final int[] mappingVigraToKNIME = createKnimeToVigraMapping(axes);

                        // create metadata of img
                        final Sourced outSourced = new DefaultSourced(SOURCE_NAME);
                        final Named outNamed = new DefaultNamed(elementPath);
                        final CalibratedSpace<CalibratedAxis> outCs = resolveCalibratedSpace(axes, mappingKNIMEToVigra);

                        // adjust dimensions
                        final long[] outDimensions = resolveDimensions(info.getDimensions(), mappingKNIMEToVigra);

                        // type info
                        final HDF5DataTypeInformation typeInformation = info.getTypeInformation();
                        final T imgType = resolveType(typeInformation);

                        // Img
                        final Img<T> img = new ArrayImgFactory<T>().create(outDimensions, imgType);

                        final MDAbstractArray<? extends Number> resolveMDArray =
                                resolveMDArray(typeInformation, reader, elementPath);

                        // create
                        if (DatasetType.IMAGE.isType(type)) {
                            fillImg(resolveMDArray, img, mappingVigraToKNIME);
                            cells.add(imageFactory.createCell(new ImgPlus<T>(img, new DefaultImgMetadata(outCs,
                                    outNamed, outSourced, new DefaultImageMetadata()))));

                        } else if (DatasetType.LABELING.isType(type)) {
                            cells.add(labelingFactory
                                    .createCell(createAndFillLabeling(resolveMDArray,
                                                                      (Img<? extends IntegerType<?>>)img,
                                                                      mappingVigraToKNIME),
                                                new DefaultLabelingMetadata(outCs, outNamed, outSourced, null)));

                        } else {
                            throw new IllegalArgumentException("this should no happen");
                        }
                    }

                    secondContainer.addRowToTable(new DefaultRow(new RowKey(dataset), cells));
                }

                secondContainer.close();

                return new BufferedDataTable[]{firstContainer.getTable(), secondContainer.getTable()};
            }

            @SuppressWarnings("unchecked")
            private T resolveType(final HDF5DataTypeInformation typeInfo) {
                T nativeType = null;
                final int elementSize = typeInfo.getElementSize();
                switch (typeInfo.getRawDataClass()) {
                    case BITFIELD:
                        nativeType = (T)new BitType();
                        break;
                    case INTEGER:
                        if (elementSize == 1) {
                            nativeType = (T)new UnsignedByteType();
                            break;
                        }

                        if (elementSize == 2) {
                            nativeType = (T)new UnsignedShortType();
                            break;
                        }

                        if (elementSize == 4) {
                            nativeType = (T)new UnsignedIntType();
                            break;
                        }
                    case FLOAT:
                        if (elementSize == 4) {
                            nativeType = (T)new FloatType();
                            break;
                        }

                        if (elementSize == 8) {
                            nativeType = (T)new DoubleType();
                            break;
                        }
                    default:
                        throw new IllegalArgumentException(
                                "We can only handle uint8, uint16, uint32 or float/double images, yet!");
                }

                return nativeType;
            }

            private MDAbstractArray<? extends Number>
                    resolveMDArray(final HDF5DataTypeInformation typeInfo, final IHDF5Reader reader,
                                   final String elementPath) {
                final int elementSize = typeInfo.getElementSize();
                switch (typeInfo.getRawDataClass()) {
                    case INTEGER:
                        if (elementSize == 1) {
                            return reader.int8().readMDArray(elementPath);
                        }

                        if (elementSize == 2) {
                            return reader.int16().readMDArray(elementPath);
                        }

                        if (elementSize == 4) {
                            return reader.int32().readMDArray(elementPath);
                        }
                    case FLOAT:
                        if (elementSize == 4) {
                            return reader.float32().readMDArray(elementPath);
                        }

                        if (elementSize == 8) {
                            return reader.float64().readMDArray(elementPath);
                        }
                    default:
                        throw new IllegalArgumentException(
                                "We can only handle uint8, uint16, uint32 or float/double images, yet!");
                }

            }

            protected void fillImg(final MDAbstractArray<? extends Number> mdArray,
                                   final Img<? extends RealType<?>> img, final int[] mapping) {
                final Cursor<? extends RealType<?>> cursor = img.cursor();

                final int[] knimePos = new int[img.numDimensions()];
                final int[] vigraPos = new int[img.numDimensions()];

                while (cursor.hasNext()) {
                    cursor.fwd();
                    cursor.localize(knimePos);

                    for (int d = 0; d < knimePos.length; d++) {
                        vigraPos[d] = knimePos[mapping[d]];
                    }

                    cursor.get().setReal(((Number)mdArray.getAsObject(vigraPos)).doubleValue());
                }
            }

            private Labeling<String>
                    createAndFillLabeling(final MDAbstractArray<? extends Number> mdArray,
                                          final Img<? extends IntegerType<?>> img, final int[] mapping) {

                @SuppressWarnings({"rawtypes", "unchecked"})
                final NativeImgLabeling<String, ?> labeling = new NativeImgLabeling(img);
                final Cursor<LabelingType<String>> cursor = labeling.cursor();

                final int[] knimePos = new int[labeling.numDimensions()];
                final int[] vigraPos = new int[labeling.numDimensions()];

                while (cursor.hasNext()) {
                    cursor.fwd();
                    cursor.localize(knimePos);

                    for (int d = 0; d < knimePos.length; d++) {
                        vigraPos[d] = knimePos[mapping[d]];
                    }

                    double val = ((Number)mdArray.getAsObject(vigraPos)).doubleValue();
                    if (val != 0) {
                        cursor.get().setLabel("" + val);
                    }
                }

                return labeling;
            }

            private long[] resolveDimensions(final long[] dimensions, final int[] mapping) {
                final long[] outDims = new long[dimensions.length];

                int k = 0;
                for (final long d : dimensions) {
                    outDims[mapping[k++]] = d;
                }

                return outDims;
            }

            private CalibratedSpace<CalibratedAxis> resolveCalibratedSpace(final ArrayList<String> axes,
                                                                           final int[] mapping) {

                final LinearAxis[] outAxes = new LinearAxis[axes.size()];

                int k = 0;
                for (final String axis : axes) {
                    outAxes[mapping[k++]] = new DefaultLinearAxis(Axes.get(axis), 1.0);
                }

                return new DefaultCalibratedSpace(outAxes);
            }

            private int[] createVigraToKnimeMapping(final List<String> axesVigra) {
                final int[] map = new int[axesVigra.size()];

                for (int j = 0; j < map.length; j++) {
                    map[j] = findLabelIndex(axesVigra.get(j));
                }

                return map;
            }

            private int[] createKnimeToVigraMapping(final List<String> axesVigra) {
                final int[] map = new int[axesVigra.size()];

                for (int j = 0; j < map.length; j++) {
                    map[findLabelIndex(axesVigra.get(j))] = j;
                }

                return map;
            }

            private int findLabelIndex(final String label) {
                for (int j = 0; j < KNIME_DIMENSION_ORDER.length; j++) {
                    if (label.equalsIgnoreCase(KNIME_DIMENSION_ORDER[j])) {
                        return j;
                    }
                }

                throw new IllegalArgumentException("Index not found!");
            }

            @Override
            protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
                    CanceledExecutionException {
                // TODO Auto-generated method stub

            }

            @Override
            protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
                    CanceledExecutionException {
                // TODO Auto-generated method stub

            }

            @Override
            protected void saveSettingsTo(final NodeSettingsWO settings) {
                m_pathModel.saveSettingsTo(settings);
            }

            @Override
            protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
                m_pathModel.validateSettings(settings);
            }

            @Override
            protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
                m_pathModel.loadSettingsFrom(settings);
            }

            @Override
            protected void reset() {
                // Nothing to dod here
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<NodeModel> createNodeView(final int viewIndex, final NodeModel nodeModel) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new DefaultNodeSettingsPane() {
            {
                addDialogComponent(new DialogComponentString(createPathModel(), "Path to Ilastik File"));
            }
        };
    }
}
