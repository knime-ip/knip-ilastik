/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by
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
 * Created on Mar 15, 2013 by hornm
 */
package org.knime.knip.ilastik.nodes.hilitebridge;

import org.knime.core.data.DoubleValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

/**
 * @author Christian Dietz, University of Konstanz
 */
public class IlastikHiliteBridgeNodeFactory extends NodeFactory<IlastikHiliteBridgeNodeModel> implements
        BufferedDataTableHolder {

    private BufferedDataTable m_data;

    /**
     * {@inheritDoc}
     */
    @Override
    public IlastikHiliteBridgeNodeModel createNodeModel() {
        return new IlastikHiliteBridgeNodeModel();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNrNodeViews() {
        return 1;
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
    @SuppressWarnings("unchecked")
    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return new DefaultNodeSettingsPane() {
            {
                createNewGroup("Position Matching");

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createXColModel(), "X Column (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createYColModel(), "Y Column (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createZColModel(), "Z Column (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createCColModel(), "Channel Column (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createTColModel(), "Time Column (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createOIdColModel(), "Object ID (optional)", 0, false, true,
                        DoubleValue.class));

                addDialogComponent(new DialogComponentColumnNameSelection(
                        IlastikHiliteBridgeNodeModel.createIlastikIdColModel(), "Ilastik ID (optional)", 0, false, true,
                        DoubleValue.class));

                createNewGroup("Communication Settings");
                addDialogComponent(new DialogComponentNumber(IlastikHiliteBridgeNodeModel.createClientPortModel(),
                        "Client Port", 1));

                addDialogComponent(new DialogComponentNumber(IlastikHiliteBridgeNodeModel.createServerPortModel(),
                        "Server Port", 1));

                addDialogComponent(new DialogComponentBoolean(
                        IlastikHiliteBridgeNodeModel.createSettingsModelBoolean(), "Keep Table in Memory"));
            }
        };
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

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<IlastikHiliteBridgeNodeModel> createNodeView(final int viewIndex,
                                                                 final IlastikHiliteBridgeNodeModel nodeModel) {
        return new IlastikHiliteBridgeNodeView(nodeModel);
    }

}
