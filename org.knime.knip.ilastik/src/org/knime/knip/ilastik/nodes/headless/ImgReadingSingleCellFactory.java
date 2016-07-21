/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2016
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

import java.io.Closeable;
import java.util.Map;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.MissingCell;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.util.Pair;
import org.knime.knip.base.data.img.ImgPlusCellFactory;
import org.knime.knip.io.ScifioImgSource;

import net.imagej.ImgPlus;
import net.imglib2.type.numeric.RealType;

/**
 *
 * @author Lorenz
 * @param <T>
 */
public class ImgReadingSingleCellFactory<T extends RealType<T>> extends SingleCellFactory implements Closeable {

    /**
     * @param newColSpec
     * @param imgPlusCellFactory
     * @param outFiles
     */
    public ImgReadingSingleCellFactory(final DataColumnSpec newColSpec, final ImgPlusCellFactory imgPlusCellFactory,
                                      final Map<RowKey, Pair<String, String>> outFiles
                                      ) {
        super(newColSpec);

        this.m_outFiles = outFiles;
        this.m_imgPlusCellFactory = imgPlusCellFactory;

        this.imgOpener = new ScifioImgSource();

    }

    final ScifioImgSource imgOpener;
    final Map<RowKey, Pair<String, String>> m_outFiles;
    final ImgPlusCellFactory m_imgPlusCellFactory;

    /**
     *
     */
    @Override
    public void close() {
        this.imgOpener.close();
    }


    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public DataCell getCell(final DataRow row) {
        RowKey key = row.getKey();
        Pair<String, String> names = m_outFiles.get(key);
        DataCell cell;
        try {
            String outfile = names.getFirst();
            String source = names.getSecond();
            ImgPlus<T> img = (ImgPlus<T>)imgOpener.getImg(outfile, 0);
            img.setSource(source);
            img.setName(key + "_result");
            cell = m_imgPlusCellFactory.createCell(img);
        } catch (Exception e) {
            cell = new MissingCell("Error during execution: " + e);
        }
        return cell;
    }
}
