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
package au.org.ala.biocache.util;

import java.awt.*;

public class ColorUtil {

	/**
	 * categorical colours
	 */
	public static final int[] colourList = {0x003366CC, 0x00DC3912, 0x00FF9900, 0x00109618, 0x00990099, 0x000099C6, 0x00DD4477,
			0x0066AA00, 0x00B82E2E, 0x00316395, 0x00994499, 0x0022AA99, 0x00AAAA11, 0x006633CC, 0x00E67300, 0x008B0707,
			0x00651067, 0x00329262, 0x005574A6, 0x003B3EAC, 0x00B77322, 0x0016D620, 0x00B91383, 0x00F4359E, 0x009C5935,
			0x00A9C413, 0x002A778D, 0x00668D1C, 0x00BEA413, 0x000C5922, 0x00743411};
	//For WMS services
	public static final String[] colorsNames = new String[]{
			"DarkRed", "IndianRed", "DarkSalmon", "SaddleBrown", "Chocolate", "SandyBrown", "Orange", "DarkGreen", "Green", "Lime", "LightGreen", "MidnightBlue", "Blue",
			"SteelBlue", "CadetBlue", "Aqua", "PowderBlue", "DarkOliveGreen", "DarkKhaki", "Yellow", "Moccasin", "Indigo", "Purple", "Fuchsia", "Plum", "Black", "White"
	};
	public static final String[] colorsCodes = new String[]{
			"8b0000", "FF0000", "CD5C5C", "E9967A", "8B4513", "D2691E", "F4A460", "FFA500", "006400", "008000", "00FF00", "90EE90", "191970", "0000FF",
			"4682B4", "5F9EA0", "00FFFF", "B0E0E6", "556B2F", "BDB76B", "FFFF00", "FFE4B5", "4B0082", "800080", "FF00FF", "DDA0DD", "000000", "FFFFFF"
	};

	public static int getRangedColour(int pos, int length) {
		int[] colourRange = {0x00002DD0, 0x00005BA2, 0x00008C73, 0x0000B944, 0x0000E716, 0x00A0FF00, 0x00FFFF00,
				0x00FFC814, 0x00FFA000, 0x00FF5B00, 0x00FF0000};

		double step = 1 / (double) colourRange.length;
		double p = pos / (double) (length);
		double dist = p / step;

		int minI = (int) Math.floor(dist);
		int maxI = (int) Math.ceil(dist);
		if (maxI >= colourRange.length) {
			maxI = colourRange.length - 1;
		}

		double minorP = p - (minI * step);
		double minorDist = minorP / step;

		//scale RGB individually
		int colour = 0x00000000;
		for (int i = 0; i < 3; i++) {
			int minC = (colourRange[minI] >> (i * 8)) & 0x000000ff;
			int maxC = (colourRange[maxI] >> (i * 8)) & 0x000000ff;
			int c = Math.min((int) ((maxC - minC) * minorDist + minC), 255);

			colour = colour | ((c & 0x000000ff) << (i * 8));
		}

		return colour;
	}

	public static String getRGB(int colour) {
		return ((colour >> 16) & 0x000000ff) + ","
				+ ((colour >> 8) & 0x000000ff) + ","
				+ (colour & 0x000000ff);
	}
	
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
