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
 * Created on 01.03.2013 by Christian Dietz
 */
package org.knime.knip.ilastik.nodes.hilitebridge;

import org.knime.core.data.RowKey;
import org.knime.core.node.NodeView;
import org.knime.core.node.property.hilite.HiLiteHandler;
import org.knime.core.node.property.hilite.HiLiteListener;
import org.knime.core.node.property.hilite.KeyEvent;

/**
 * Node View for Ilastik Hilite Bridge
 *
 * @author Christian Dietz, University of Konstanz
 * @author Andreas Graumann, University of Konstanz
 */
public class IlastikHiliteBridgeNodeView extends NodeView<IlastikHiliteBridgeNodeModel> {

    /**
     * JPanel for Gui
     */
    private IlastikHiliteBridgeViewPanel m_gui; //GUI components (is a JPanel)

    /**
     * Node Model
     */
    private IlastikHiliteBridgeNodeModel m_nodeModel; //NodeModel

    /**
     * Hilite Listener
     */
    private HiLiteListener m_hiliteListener;

    /**
     * Hilite Handler
     */
    private HiLiteHandler m_inHiLiteHandler;

    /**
     * Ilastik Hilite Client
     */
    private DefaultIlastikHiliteClient m_client;

    /**
     * Ilastik Hilite Server
     */
    private IlastikHiliteServer m_server;

    /**
     * Constructor
     *
     * @param _model
     *
     */
    protected IlastikHiliteBridgeNodeView(final IlastikHiliteBridgeNodeModel _model) {
        super(_model);

        // get NodelModel
        m_nodeModel = getNodeModel();

        // create client
        m_client = new DefaultIlastikHiliteClient(m_nodeModel.getClientPort());

        // create Server
        m_server = new IlastikHiliteServer(m_nodeModel.getServerPort(), m_client, m_nodeModel);

        //initializes gui
        initView();
    }

    /**
     *
     */
    private void initLogic() {

        // remove all hilite listener so far, start from sratch
        if (m_inHiLiteHandler != null) {
            m_inHiLiteHandler.removeHiLiteListener(m_hiliteListener);
        }

        // get hilite handler
        m_inHiLiteHandler = m_nodeModel.getInHiLiteHandler(0);

        // update hilites in JPanel
        m_gui.updateHilites(m_inHiLiteHandler.getHiLitKeys());

        // create a new hilite listener
        m_hiliteListener = new HiLiteListener() {

            @Override
            public void unHiLiteAll(final KeyEvent event) {
                m_gui.clearAllHilites();
            }

            @Override
            public void unHiLite(final KeyEvent event) {
                m_gui.clearHilites(event.keys());
            }

            @Override
            public void hiLite(final KeyEvent event) {
                m_gui.updateHilites(event.keys());
            }
        };

        // add this hilite listener
        m_inHiLiteHandler.addHiLiteListener(m_hiliteListener);

        // register listener add server
        m_server.registerListener(new IlastikHiliteListenerServer() {

            @Override
            public void keyUnhilited(final String key) {

                m_inHiLiteHandler.fireUnHiLiteEvent(new RowKey(key));
            }

            @Override
            public void keyHilited(final String key) {
                m_inHiLiteHandler.fireHiLiteEvent(new RowKey(key));
            }

            @Override
            public void clearHilites() {
                m_inHiLiteHandler.fireClearHiLiteEvent();
            }

            @Override
            public void toggleHilite(final String key) {
                if (m_inHiLiteHandler.isHiLit(new RowKey(key))) {
                    m_inHiLiteHandler.fireUnHiLiteEvent(new RowKey(key));
                }
                else {
                    m_inHiLiteHandler.fireHiLiteEvent(new RowKey(key));
                }

            }
        });
    }

    /**
     * initialize GUI
     */
    private void initView() {
        m_gui = new IlastikHiliteBridgeViewPanel(m_nodeModel.getPositionAccess(), m_client, m_server, m_nodeModel.getMapPosAcces());
        setComponent(m_gui);
    }

    @Override
    protected void onClose() {
        m_inHiLiteHandler.removeHiLiteListener(m_hiliteListener);
        m_client.closeConnection();
        m_server.shutDown();
    }

    @Override
    protected void onOpen() {
        if (m_nodeModel == null) {
            return;
        }
    }

    @Override
    protected void modelChanged() {
        initLogic();
    }
}
