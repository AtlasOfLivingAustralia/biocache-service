/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package org.ala.biocache.util;

import java.awt.Color;

public class ColorUtil {
	
	/**
	 * Translates a "ff0000" html color into an AWT color.
	 * 
	 * @param htmlRGB
	 * @param opacity
	 * @return java.awt.Color
	 */
	public static Color getColor(String htmlRGB, Float opacity){
		if(htmlRGB == null || htmlRGB.length() != 6){
			throw new IllegalArgumentException("badly formatted RGB string: " + htmlRGB);
		}

        int red = Integer.parseInt(htmlRGB.substring(0, 2), 16);
        int green = Integer.parseInt(htmlRGB.substring(2, 4), 16);
        int blue = Integer.parseInt(htmlRGB.substring(4), 16);
        int alpha = (int) (255 * opacity);

        Integer colour = (red << 16) | (green << 8) | blue;
        colour = colour | (alpha << 24);		
		return new Color(colour, true);
	}
}
