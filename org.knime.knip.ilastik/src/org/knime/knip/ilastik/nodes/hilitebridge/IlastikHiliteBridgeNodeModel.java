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
 * Created on 15.05.2014 by Christian Dietz
 */
package org.knime.knip.ilastik.nodes.hilitebridge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.RowKey;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.knip.ilastik.nodes.NodeTools;

/**
 * Node Model for Ilastik Hilite Bridge
 *
 * @author Andreas Graumann, University of Konstanz
 * @author Christian Dietz, Univesity of Konstanz
 */
public class IlastikHiliteBridgeNodeModel extends NodeModel {

    /**
     *
     */
    protected IlastikHiliteBridgeNodeModel() {
        super(1, 0);
    }

    /**
     * Map with all positions
     */
    private MapPositionAccess m_positionAccess;

    /**
     * Keep table in memory
     */
    private SettingsModelBoolean m_keepInMemory = createSettingsModelBoolean();

    /**
     * Settings Model for X Column
     */
    private SettingsModelString m_xColModel = createXColModel();

    /**
     * Settings Model for Y Column
     */
    private SettingsModelString m_yColModel = createYColModel();

    /**
     * Settings Model for Z Column
     */
    private SettingsModelString m_zColModel = createZColModel();

    /**
     * Settings Model for Channel Column
     */
    private SettingsModelString m_cColModel = createCColModel();

    /**
     * Settings Model for Time Column
     */
    private SettingsModelString m_tColModel = createTColModel();

    /**
     * Settings Model for IlastikID Column
     */
    private SettingsModelString m_ilastikIdColModel = createIlastikIdColModel();

    /**
     * Settings Model for ObjectId Column
     */
    private SettingsModelString m_oIdColModel = createOIdColModel();

    /**
     * Settings Model for Client Port
     */
    private SettingsModelInteger m_clientPortModel = createClientPortModel();

    /**
     * Settings Model for Server Port
     */
    private SettingsModelInteger m_serverPortModel = createServerPortModel();

    /**
     * Data Table
     */
    private BufferedDataTable[] m_inData;

    /**
     *
     */
    final Map<RowKey, double[]> m_positionMap = new HashMap<RowKey, double[]>();

    /**
     *
     */
    final Map<RowKey, Double> m_objectIdMap = new HashMap<RowKey, Double>();


    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        return null;
    }


    static SettingsModelString createOIdColModel() {
        return new SettingsModelString("oid_model", "");
    }

    static SettingsModelString createTColModel() {
        return new SettingsModelString("t_model", "");
    }

    static SettingsModelString createIlastikIdColModel() {
        return new SettingsModelString("ilastikId_model", "");
    }

    static SettingsModelString createCColModel() {
        return new SettingsModelString("c_model", "");
    }

    static SettingsModelString createZColModel() {
        return new SettingsModelString("z_model", "");
    }

    static SettingsModelString createYColModel() {
        return new SettingsModelString("y_model", "");
    }

    static SettingsModelString createXColModel() {
        return new SettingsModelString("x_model", "");
    }

    static SettingsModelInteger createServerPortModel() {
        return new SettingsModelInteger("server_port", 9997);
    }

    static SettingsModelInteger createClientPortModel() {
        return new SettingsModelInteger("client_port", 9998);
    }

    static SettingsModelBoolean createSettingsModelBoolean() {
        return new SettingsModelBoolean("keep_in_memory", true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
            throws Exception {
        m_positionAccess = getPositionAccess(inData);
        m_inData = inData;

        return null;
    }

    /**
     * @param inData
     * @return
     *      Map with all positions
     * @throws InvalidSettingsException
     */
    private MapPositionAccess getPositionAccess(final BufferedDataTable[] inData) throws InvalidSettingsException {
        //
        int xCol = resolveIdx(m_xColModel, inData[0].getDataTableSpec());
        int yCol = resolveIdx(m_yColModel, inData[0].getDataTableSpec());
        int zCol = resolveIdx(m_zColModel, inData[0].getDataTableSpec());
        int cCol = resolveIdx(m_cColModel, inData[0].getDataTableSpec());
        int tCol = resolveIdx(m_tColModel, inData[0].getDataTableSpec());
        int oIdCol = resolveIdx(m_oIdColModel, inData[0].getDataTableSpec());
        int iIdCol = resolveIdx(m_ilastikIdColModel, inData[0].getDataTableSpec());

        // here we simply read in the data and remember the position data.

        for (final DataRow row : inData[0]) {
            final double[] pos = new double[6];


            if (xCol != -1) {
                pos[0] = ((DoubleValue)row.getCell(xCol)).getDoubleValue();
            }

            if (yCol != -1) {
                pos[1] = ((DoubleValue)row.getCell(yCol)).getDoubleValue();
            }

            if (zCol != -1) {
                pos[2] = ((DoubleValue)row.getCell(zCol)).getDoubleValue();


           }   if (cCol != -1) {
                pos[3] = ((DoubleValue)row.getCell(cCol)).getDoubleValue();
            }

            if (tCol != -1) {
                pos[4] = ((DoubleValue)row.getCell(tCol)).getDoubleValue();
            }

            if(iIdCol != -1) {
                pos[5] = ((DoubleValue)row.getCell(iIdCol)).getDoubleValue();
            }

            if (oIdCol != -1) {
                Double oId = ((DoubleValue)row.getCell(oIdCol)).getDoubleValue();
                m_objectIdMap.put(row.getKey(), oId);
            }

            m_positionMap.put(row.getKey(), pos);
        }

        return new MapPositionAccess(m_positionMap);
    }

    /**
     *
     * Get Row Key for row with time = time_id and ilastik_id
     *
     * @param iliast_id
     * @param time_id
     * @return
     *      RowKey
     * @throws InvalidSettingsException
     */
   public RowKey resolveRowIds(final int iliast_id, final int time_id) throws InvalidSettingsException{

       int time_idx = resolveIdx(m_tColModel, m_inData[0].getDataTableSpec());
       int ilastik_idx = resolveIdx(m_ilastikIdColModel, m_inData[0].getDataTableSpec());

       for (final DataRow row : m_inData[0]) {

           long t = ((LongValue)row.getCell(time_idx)).getLongValue();
           long i = ((LongValue)row.getCell(ilastik_idx)).getLongValue();

           if (t == time_id && i == iliast_id) {
            return row.getKey();
        }

       }
       return null;
   }

   /**
    *
    * Get list of row keys which conforms to the given attributes
    *
    * @param attr
    *       Map with all attributes to resolve the row ids
    * @param and
    *       Mode: False = OR, True = AND
    * @return
    *       Map with all desired RowKeys
    */
   public ArrayList<RowKey> resolveRowIdsByMap(final HashMap<String, Integer> attr, final boolean and) {

       ArrayList<RowKey> keys = new ArrayList<RowKey>();

       // run over all rows
       for (final DataRow row : m_inData[0]) {

           boolean validRow = false;

           for (Map.Entry<String, Integer> e : attr.entrySet()) {
               String colName = e.getKey();
               long desiredValue = e.getValue();


               int colIdx = getColumnIdxByName(colName, m_inData[0].getDataTableSpec());

               if (colIdx == -1) {
                   break;
               }

               long givenValue = ((LongValue)row.getCell(colIdx)).getLongValue();

               if (desiredValue == givenValue) {
                   validRow = true;
                   if (!and) {
                       break;
                   }
               } else {
                   validRow = false;
               }
           }
           if (validRow) {
               keys.add(row.getKey());
           }

        }


       return keys;
   }

   /**
    *
    * @param colName
    *       Name of searched column
    * @param spec
    *       DataTableSpec
    * @return
    */
   private int getColumnIdxByName(final String colName, final DataTableSpec spec) {

       if (!spec.containsName(colName)) {
           return -1;
       }

       return spec.findColumnIndex(colName);
   }

   /**
    * Resolve column index
    *
    * @param model
    * @param spec
    * @return
    *       index
    * @throws InvalidSettingsException
    */
   private int resolveIdx(final SettingsModelString model, final DataTableSpec spec) throws InvalidSettingsException {
        if (model.getStringValue() == null) {
            return -1;
        }
        return NodeTools.autoColumnSelection(spec, model, DoubleValue.class, this.getClass());
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {

    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {

    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_keepInMemory.saveSettingsTo(settings);
        m_clientPortModel.saveSettingsTo(settings);
        m_cColModel.saveSettingsTo(settings);
        m_tColModel.saveSettingsTo(settings);
        m_xColModel.saveSettingsTo(settings);
        m_yColModel.saveSettingsTo(settings);
        m_zColModel.saveSettingsTo(settings);
        m_oIdColModel.saveSettingsTo(settings);
        m_serverPortModel.saveSettingsTo(settings);
        m_ilastikIdColModel.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_keepInMemory.validateSettings(settings);
        m_clientPortModel.validateSettings(settings);
        m_cColModel.validateSettings(settings);
        m_tColModel.validateSettings(settings);
        m_xColModel.validateSettings(settings);
        m_yColModel.validateSettings(settings);
        m_zColModel.validateSettings(settings);
        m_oIdColModel.validateSettings(settings);
        m_serverPortModel.validateSettings(settings);
        m_ilastikIdColModel.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_keepInMemory.loadSettingsFrom(settings);
        m_clientPortModel.loadSettingsFrom(settings);
        m_serverPortModel.loadSettingsFrom(settings);
        m_cColModel.loadSettingsFrom(settings);
        m_tColModel.loadSettingsFrom(settings);
        m_xColModel.loadSettingsFrom(settings);
        m_yColModel.loadSettingsFrom(settings);
        m_zColModel.loadSettingsFrom(settings);
        m_oIdColModel.loadSettingsFrom(settings);
        m_ilastikIdColModel.loadSettingsFrom(settings);
    }

    @Override
    protected void reset() {

    }

    /**
     * @return position access for this model
     */
    public PositionAccess getPositionAccess() {
        return m_positionAccess;
    }

    /**
     * @return port number
     */
    public int getClientPort() {
        return m_clientPortModel.getIntValue();
    }

    /**
     * @return
     *          serverPort
     */
    public int getServerPort() {
        return m_serverPortModel.getIntValue();
    }

    /**
     *
     * @return
     *      all Positions as Map
     */
    public Map<RowKey, double[]> getPositionMap() {
        return m_positionMap;
    }

    /**
     *
     * @return
     *      all object Ids as Map
     */
    public Map<RowKey, Double> getObjectIdMap() {
       return m_objectIdMap;
    }


    /**
     *
     * @return
     *      map with all positions
     */
    public MapPositionAccess getMapPosAcces() {
        return m_positionAccess;
    }

}