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
import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
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
 *
 * @author Christian Dietz
 */
public class IlastikHiliteBridgeNodeModel extends NodeModel {

    /**
     *
     */
    protected IlastikHiliteBridgeNodeModel() {
        super(1, 0);
    }

    private MapPositionAccess m_positionAccess;

    private SettingsModelBoolean m_keepInMemory = createSettingsModelBoolean();

    private SettingsModelString m_xColModel = createXColModel();

    private SettingsModelString m_yColModel = createYColModel();

    private SettingsModelString m_zColModel = createZColModel();

    private SettingsModelString m_cColModel = createCColModel();

    private SettingsModelString m_tColModel = createTColModel();

    private SettingsModelInteger m_clientPortModel = createClientPortModel();

    private SettingsModelInteger m_serverPortModel = createServerPortModel();

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        return null;
    }

    static SettingsModelString createTColModel() {
        return new SettingsModelString("t_model", "");
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
        return new SettingsModelInteger("server_port", 13371);
    }

    static SettingsModelInteger createClientPortModel() {
        return new SettingsModelInteger("client_port", 13370);
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

        return null;
    }

    /**
     * @param inData
     * @return
     * @throws InvalidSettingsException
     */
    private MapPositionAccess getPositionAccess(final BufferedDataTable[] inData) throws InvalidSettingsException {
        //
        int xCol = resolveIdx(m_xColModel, inData[0].getDataTableSpec());
        int yCol = resolveIdx(m_yColModel, inData[0].getDataTableSpec());
        int zCol = resolveIdx(m_zColModel, inData[0].getDataTableSpec());
        int cCol = resolveIdx(m_cColModel, inData[0].getDataTableSpec());
        int tCol = resolveIdx(m_tColModel, inData[0].getDataTableSpec());

        // here we simply read in the data and remember the position data.

        // X,Y,Z,C,T mapping
        final Map<RowKey, double[]> map = new HashMap<RowKey, double[]>();

        //TODO use array here too
        for (final DataRow row : inData[0]) {
            final double[] pos = new double[5];

            if (xCol != -1) {
                pos[0] = ((DoubleValue)row.getCell(xCol)).getDoubleValue();
            }

            if (yCol != -1) {
                pos[1] = ((DoubleValue)row.getCell(yCol)).getDoubleValue();
            }

            if (zCol != -1) {
                pos[2] = ((DoubleValue)row.getCell(zCol)).getDoubleValue();
            }

            if (cCol != -1) {
                pos[3] = ((DoubleValue)row.getCell(cCol)).getDoubleValue();
            }

            if (tCol != -1) {
                pos[4] = ((DoubleValue)row.getCell(tCol)).getDoubleValue();
            }

            map.put(row.getKey(), pos);
        }

        return new MapPositionAccess(map);
    }

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
        m_serverPortModel.saveSettingsTo(settings);
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
        m_serverPortModel.validateSettings(settings);
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
    }

    @Override
    protected void reset() {
        //        if (m_ilastikListener != null) {
        //            IlastikHiliteServer.getInstance().unregisterListener(m_ilastikListener);
        //        }
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
     */
    public int getServerPort() {
        return m_serverPortModel.getIntValue();
    }

}