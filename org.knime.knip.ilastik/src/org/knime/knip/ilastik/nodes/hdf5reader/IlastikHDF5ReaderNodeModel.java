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

import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import net.imagej.ImgPlus;
import net.imagej.Sourced;
import net.imagej.axis.Axes;
import net.imagej.axis.CalibratedAxis;
import net.imagej.axis.DefaultLinearAxis;
import net.imagej.axis.LinearAxis;
import net.imagej.space.CalibratedSpace;
import net.imagej.space.DefaultCalibratedSpace;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.logic.BitType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
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
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
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
import org.knime.core.node.property.hilite.HiLiteHandler;
import org.knime.knip.base.data.img.ImgPlusCell;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.core.data.DefaultNamed;
import org.knime.knip.core.data.DefaultSourced;
import org.knime.knip.core.data.img.DefaultImageMetadata;
import org.knime.knip.core.data.img.DefaultImgMetadata;
import org.scijava.Named;

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
 *
 * Simple hdf5 reader for ilastik hdf5 files
 *
 * @author Andreas Graumann, University of Konstanz
 * @author Christian Dietz, University of Konstanz
 * @param <T>
 */
@SuppressWarnings("deprecation")
public class IlastikHDF5ReaderNodeModel<T extends NativeType<T> & RealType<T>> extends NodeModel {

    SettingsModelString m_fileChooser = createFileChooserModel();

    // Constants for reading
    final static String[] KNIME_DIMENSION_ORDER = new String[]{"X", "Y", "Z", "C", "Time"};

    /**
     *
     */
    final static String TYPE_ATTRIBUTE = "type";

    /**
     *
     */
    final static String AXIS_ATTRIBUTE = "axistags";

    /**
     *
     */
    final static String RAW_IMAGE_PATH = "images/raw";

    /**
     *
     */
    final static String IMG_FOLDER_NAME = "images";

    /**
     *
     */
    final static String TABLE_NAME = "table";

    /**
     *
     */
    final static String TRACKING_NAME = "divisions";

    // Constants for output

    /**
     *
     */
    final static String IMAGE_COL_PREFIX = "Raw Image";

    /**
     *
     */
    final static String LABELING_COL_PREFIX = "Labeling";

    /**
     *
     */
    final static String LABELING_NAME = "labeling";

    /**
     *
     */
    final static String RAW_PATCH_NAME = "raw";

    /**
     *
     */
    final static String SOURCE_NAME = "Ilastik";

    /**
     * do we have raw image rois
     */
    boolean rawImagesPatches = false;

    /**
     * tracking data set included?
     */
    boolean containsTrackingData = false;

    /**
     * columns to add to feature table, because of labeling and raw image roi
     */
    int addColumnsToFeatureTable = 1;

    /**
     * Hilite handler for first output
     */
    private final HiLiteHandler h0 = new HiLiteHandler();

    /**
     * Hilite handler for second output
     */
    private final HiLiteHandler h1 = new HiLiteHandler();

    /**
     * Hilite handler for third output
     */
    private final HiLiteHandler h2 = new HiLiteHandler();

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

    @Override
    public HiLiteHandler getOutHiLiteHandler(final int outIndex) {
        if (outIndex == 0) {
            return h0;
        }

        if (outIndex == 1) {
            return h1;
        }

        if (outIndex == 2) {
            return h2;
        }

        return null;
    };

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        rawImagesPatches = false;
        containsTrackingData = false;
        addColumnsToFeatureTable = 1;
        if (new File(m_fileChooser.getStringValue()).exists()) {
            return new DataTableSpec[]{createFeatureOutputSpec(), createTrackingDataOutputSpec(), createRawImageOutputSpec()};
        } else {
            return null;
        }
    }

    /**
     * Creates Data table spec for output table
     *
     * @return Data table spec for feature table with image patches
     */
    private DataTableSpec createFeatureOutputSpec() {

        addColumnsToFeatureTable = 1;
        // get HDF5 Reader
        final IHDF5Reader reader = HDF5Factory.openForReading(m_fileChooser.getStringValue());
        // get Data set information for column specs
        final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);

        // try finding the whole raw image, when it works we dont have raw image patches


        if (!reader.exists(IMG_FOLDER_NAME + "/raw")) {
            // yes we have raw image patches
            rawImagesPatches = true;
            // we need one more column
            addColumnsToFeatureTable++;
        }

        // create data column spec
        final DataColumnSpec[] specs = new DataColumnSpec[dataSetInfo.length + addColumnsToFeatureTable];

        // counter for columns
        int i = 0;
        for (final HDF5CompoundMemberInformation memberInfo : dataSetInfo) {
            specs[i] = new DataColumnSpecCreator(memberInfo.getName(), typeMatcher(memberInfo.getType())).createSpec();
            i++;
        }

        // create labeling column spec
        specs[i] = new DataColumnSpecCreator("BitMask", ImgPlusCell.TYPE).createSpec();

        // create imgplus column spec, if needed
        if (rawImagesPatches) {
            specs[i + 1] = new DataColumnSpecCreator("Raw Image Patch", ImgPlusCell.TYPE).createSpec();
        }

        return new DataTableSpec(specs);
    }

    /**
     * Create data table spec for tracking data table in hdf5 file
     *
     * @return Data table spec for tracking data
     */
    private DataTableSpec createTrackingDataOutputSpec() {

        // get HDF5 Reader
        final IHDF5Reader reader = HDF5Factory.openForReading(m_fileChooser.getStringValue());
        // get Data set information for column specs
         HDF5CompoundMemberInformation[] dataSetInfo = null;
         DataColumnSpec[] specs = null;

        // check if table exists
        if (reader.exists(TRACKING_NAME)) {
            containsTrackingData = true;
            dataSetInfo = reader.compound().getDataSetInfo(TRACKING_NAME);
            specs = new DataColumnSpec[dataSetInfo.length];

            // counter for columns
            int i = 0;
            for (final HDF5CompoundMemberInformation memberInfo : dataSetInfo) {
                specs[i] = new DataColumnSpecCreator(memberInfo.getName(), typeMatcher(memberInfo.getType())).createSpec();
                i++;
            }

        }
        else {
            specs = new DataColumnSpec[0];
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
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {

        // container for output data
        BufferedDataContainer featureContainer = null;
        BufferedDataContainer rawImageContainer = null;
        BufferedDataContainer trackingContainer = null;

        // check if file is valid
        if (new File(m_fileChooser.getStringValue()).exists()) {
            featureContainer = exec.createDataContainer(createFeatureOutputSpec());
            rawImageContainer = exec.createDataContainer(createRawImageOutputSpec());
            trackingContainer = exec.createDataContainer(createTrackingDataOutputSpec());
        } else {
            throw new IllegalArgumentException("Path is not a .h5 file");
        }

        // open hdf5 file
        final IHDF5Reader reader = HDF5Factory.openForReading(m_fileChooser.getStringValue());
        final IHDF5CompoundReader compound = reader.compound();

        fillFeatureTable(exec, featureContainer, reader, compound);
        // close feature container
        featureContainer.close();

        if (containsTrackingData) {
            fillTrackingDataTable(exec, trackingContainer, reader, compound);
        }
        // close tracking container
        trackingContainer.close();

        if (!rawImagesPatches) {
            final IHDF5StringReader stringReader = reader.string();
            final String elementRawImagePath = IMG_FOLDER_NAME + "/raw";
            final DataCell[] cells = new DataCell[1];
            cells[0] = createImgPlusCell(exec, reader, elementRawImagePath, stringReader, false);
            rawImageContainer.addRowToTable(new DefaultRow("RawImage", cells));
        }

        rawImageContainer.close();

        return new BufferedDataTable[]{featureContainer.getTable(), trackingContainer.getTable(), rawImageContainer.getTable()};
    }

    /**
     * @param exec
     *              Execution context
     * @param trackingContainer
     *              Container to fill with tracking data
     * @param reader
     *              HdF5 Reader
     * @param compound
     * @throws IOException
     */
    private void fillTrackingDataTable(final ExecutionContext exec, final BufferedDataContainer trackingContainer,
                                  final IHDF5Reader reader, final IHDF5CompoundReader compound) throws IOException {
        final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TRACKING_NAME);
        // store the list
        final HDF5CompoundDataMap[] map = compound.readArray(TRACKING_NAME, HDF5CompoundDataMap.class);

        // create row
        for (int i = 0; i < map.length; i++) {

            // create array with data cells
            final DataCell[] cells = new DataCell[map[i].size()];

            for (int j = 0; j < dataSetInfo.length; j++) {

                HDF5CompoundMemberInformation info = dataSetInfo[j];

                Class<?> cellType = clazzMatcher(info.getType());

                // create data cell;
                cells[j] = createDataCell(cellType, map[i].get(info.getName()));
            }

            // add row to table
            trackingContainer.addRowToTable(new DefaultRow("Row" + i, cells));

        }
    }

    /**
     * @param exec
     * @param featureContainer
     * @param reader
     * @param compound
     * @param dataSetInfo
     * @throws IOException
     */
    private void fillFeatureTable(final ExecutionContext exec, final BufferedDataContainer featureContainer,
                                  final IHDF5Reader reader, final IHDF5CompoundReader compound) throws IOException {

        final HDF5CompoundMemberInformation[] dataSetInfo = reader.compound().getDataSetInfo(TABLE_NAME);
        // store the list
        final HDF5CompoundDataMap[] map = compound.readArray(TABLE_NAME, HDF5CompoundDataMap.class);

        // create row
        for (int i = 0; i < map.length; i++) {

            // create array with data cells
            final DataCell[] cells = new DataCell[map[i].size() + addColumnsToFeatureTable];

            for (int j = 0; j < dataSetInfo.length; j++) {

                HDF5CompoundMemberInformation info = dataSetInfo[j];

                Class<?> cellType = clazzMatcher(info.getType());

                // create data cell;
                cells[j] = createDataCell(cellType, map[i].get(info.getName()));
            }

            final IHDF5StringReader stringReader = reader.string();
            final String elementPathBitMask = IMG_FOLDER_NAME + "/" + i + "/" + LABELING_NAME;
            cells[map[i].size()] = createImgPlusCell(exec, reader, elementPathBitMask, stringReader, true);

            if (rawImagesPatches) {
                final String elementPathImage = IMG_FOLDER_NAME + "/" + i + "/" + RAW_PATCH_NAME;
                cells[map[i].size()+1] = createImgPlusCell(exec, reader, elementPathImage, stringReader, false);
            }

            // add row to table
            featureContainer.addRowToTable(new DefaultRow("Row" + i, cells));

        }

        // close feature container
        featureContainer.close();
    }

    /**
    *
    * @param exec
    * @param reader
    * @param i
    * @param stringReader
    * @return
    * @throws IOException
    */
   @SuppressWarnings("unchecked")
private ImgPlusCell<T> createImgPlusCell(final ExecutionContext exec, final IHDF5Reader reader, final String elementPath,
                                   final IHDF5StringReader stringReader, final boolean isLabeling) throws IOException {

       final HDF5DataSetInformation info = reader.getDataSetInformation(elementPath);

       final ArrayList<String> axes = getAxex(stringReader, elementPath);

       final ImgPlusCellFactory imageFactory = new ImgPlusCellFactory(exec);

       // type info
       final HDF5DataTypeInformation typeInformation = info.getTypeInformation();
       T imgType = resolveType(typeInformation);

       if (isLabeling) {
           imgType = (T) new BitType();
       }



       final Sourced outSourced = new DefaultSourced(SOURCE_NAME);
       final Named outNamed = new DefaultNamed(elementPath);

       // just one pixel
       if (axes.size() == 0) {
           final long dim[] = new long[]{1};
           final Img<T> i = new ArrayImgFactory<T>().create(dim, imgType);
           LinearAxis axe = new  DefaultLinearAxis(Axes.get("X"), 1.0);
           final CalibratedSpace<CalibratedAxis> cs = new DefaultCalibratedSpace(axe);
           return imageFactory.createCell(new ImgPlus<T>(i, new DefaultImgMetadata(cs, outNamed,
                                                                                     outSourced, new DefaultImageMetadata())));
       }

       final MDAbstractArray<? extends Number> resolveMDArray =
               resolveMDArray(typeInformation, reader, elementPath);

       // Mapping to map dimensions labels from knime <-> ilastik
       final int[] mappingIlastikToKnime = createIlastikToKnimeMapping(axes);
       final int[] mappingKnimeToIlastik = createKnimeToIlastikMapping(axes);

       // create metadata of img
       final CalibratedSpace<CalibratedAxis> outCs = resolveCalibratedSpace(axes, mappingIlastikToKnime, mappingKnimeToIlastik);

       // adjust dimensions
       final long[] outDimensions = resolveDimensions(info.getDimensions(), mappingIlastikToKnime, mappingKnimeToIlastik);

       final Img<T> img = new ArrayImgFactory<T>().create(outDimensions, imgType);

       fillImg(resolveMDArray, img, mappingIlastikToKnime, mappingKnimeToIlastik);
       return imageFactory.createCell(new ImgPlus<T>(img, new DefaultImgMetadata(outCs, outNamed,
                                                                              outSourced, new DefaultImageMetadata())));
   }

    /**
     *
     * @param mdArray
     * @param img
     * @param mapping
     * @param mapping2
     */
    protected void fillImg(final MDAbstractArray<? extends Number> mdArray, final Img<? extends RealType<?>> img,
                           final int[] mapping, final int[] mapping2) {
        final Cursor<? extends RealType<?>> cursor = img.cursor();

        final int[] knimePos = new int[img.numDimensions()];
        final int[] ilastikPos = new int[img.numDimensions()];

        while (cursor.hasNext()) {
            cursor.fwd();
            cursor.localize(knimePos);

            for (int d = 0; d < knimePos.length; d++) {
                ilastikPos[d] = knimePos[mapping2[mapping[d]]];
            }

            cursor.get().setReal(((Number)mdArray.getAsObject(ilastikPos)).doubleValue());
        }
    }



    /**
     *
     * @param stringReader
     * @param elementPath
     * @return
     */
    private ArrayList<String> getAxex(final IHDF5StringReader stringReader, final String elementPath) {
        final JsonParser jsonParser =
                Json.createParser(new CharArrayReader(stringReader.getAttr(elementPath, AXIS_ATTRIBUTE).toCharArray()));

        final ArrayList<String> axes = new ArrayList<String>();
        while (jsonParser.hasNext()) {
            final Event next = jsonParser.next();
            switch (next) {
                case KEY_NAME:
                    if (jsonParser.getString().equalsIgnoreCase("key")) {
                        jsonParser.next();
                        axes.add(jsonParser.getString().toUpperCase());
                    }
                    break;
                default:
            }
        }
        return axes;
    }

    /**
     *
     * @param axes
     * @param mapping
     * @return
     */
    private CalibratedSpace<CalibratedAxis> resolveCalibratedSpace(final ArrayList<String> axes, final int[] mapping, final int[] mapping2) {

        final LinearAxis[] outAxes = new LinearAxis[axes.size()];

        int k = 0;
        for (final String axis : axes) {
            outAxes[mapping2[mapping[k++]]] = new DefaultLinearAxis(Axes.get(axis), 1.0);
        }

        return new DefaultCalibratedSpace(outAxes);
    }

    private MDAbstractArray<? extends Number> resolveMDArray(final HDF5DataTypeInformation typeInfo,
                                                             final IHDF5Reader reader, final String elementPath) {

        final int elementSize = typeInfo.getElementSize();
        switch (typeInfo.getRawDataClass()) {
            case INTEGER:
                if (elementSize == 1) {
                    return reader.int64().readMDArray(elementPath);
                }

                if (elementSize == 2) {
                    return reader.int64().readMDArray(elementPath);
                }

                if (elementSize == 4) {
                    return reader.int32().readMDArray(elementPath);
                }

                if (elementSize == 8) {
                    return reader.int64().readMDArray(elementPath);
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

    /**
     *
     * @param dimensions
     * @param mapping
     * @return
     */
    private long[] resolveDimensions(final long[] dimensions, final int[] mapping, final int[] mapping2) {
        final long[] outDims = new long[dimensions.length];

        int k = 0;
        for (final long d : dimensions) {
            outDims[mapping2[mapping[k++]]] = d;
        }

        return outDims;
    }

    /**
     *
     * @param axesIlastik
     * @return
     */
    private int[] createIlastikToKnimeMapping(final List<String> axesIlastik) {
        final int[] map = new int[axesIlastik.size()];

        for (int j = 0; j < map.length; j++) {
            map[j] = findLabelIndex(axesIlastik.get(j));
        }

        return map;
    }

    /**
     *
     * @param axesIlastik
     * @return
     */
    private int[] createKnimeToIlastikMapping(final List<String> axesIlastik) {
//        final int[] map = new int[axesIlastik.size()];
        final int[] map = new int[KNIME_DIMENSION_ORDER.length];
        Arrays.fill(map, -1);

        for (int j = 0; j < axesIlastik.size(); j++) {

            int index = findLabelIndex(axesIlastik.get(j));
            map[index] = j;
        }

        return map;
    }

    /**
     *
     * @param label
     * @return
     */
    private int findLabelIndex(final String label) {
        for (int j = 0; j < KNIME_DIMENSION_ORDER.length; j++) {
            if (label.equalsIgnoreCase(KNIME_DIMENSION_ORDER[j])) {
                return j;
            }
        }

        throw new IllegalArgumentException("Index not found!");
    }

    /**
     *
     * @param typeInfo
     * @return
     */
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

                if (elementSize == 8) {
                    nativeType = (T)new LongType();
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

    /**
     *
     * @param cellType
     * @param value
     * @return
     */
    private DataCell createDataCell(final Class<?> cellType, final Object value) {
        if (cellType.equals(Byte.class)) {
            return new IntCell((Byte)value);
        } else if (cellType.equals(Float.class)) {
            return new DoubleCell((Float)value);
        } else if (cellType.equals(Double.class)) {
            return new DoubleCell((Double)value);
        } else if (cellType.equals(Short.class)) {
            return new IntCell((Short)value);
        } else if (cellType.equals(Integer.class)) {
            return new IntCell((Integer)value);
        } else if (cellType.equals(Long.class)) {
            return new LongCell((Long)value);
        } else if (cellType.equals(String.class)) {
            return new StringCell((String)value);
        } else {
            throw new IllegalArgumentException("Can't read feature table, unknown type");
        }
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
                if (type.getElementSize() <= 4) {
                    return IntCell.TYPE;
                }
                if (type.getElementSize() == 8) {
                    return LongCell.TYPE;
                }
                new InvalidSettingsException("Can't match HDF5DataType to KNIME DataType");
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
                if (type.getElementSize() == 8) {
                    return Long.class;
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
