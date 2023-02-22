/**************************************************************************
 *  Copyright (C) 2011 Atlas of Living Australia
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
package au.org.ala.biocache.service;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

/**
 * Offline integration test for spatial-service.
 * <p>
 * Requirements
 * - /test/docker SOLR and CASSANDRA
 * - /src/test/resources/biocache-test-config.properties with valid layers.service.url (spatial-service)
 * - spatial-service contains fields `aus`, `aus2`
 * - spatial-service contains distributions for LSID `urn:lsid:biodiversity.org.au:afd.taxon:8dd6937f-1032-43d3-a75c-da404515743b`
 * - spatial-service contains checklists for LSID `urn:lsid:biodiversity.org.au:afd.taxon:e617b4ec-e848-4759-bc1f-60d085f00fe5`
 * - spatial-service contains no tracks for LSID `1`
 */
@Ignore("Run this test offline")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:springTest.xml"})
@WebAppConfiguration
@TestPropertySource(locations = "classpath:biocache-test-config.properties")
public class SpatialServiceIntegrationTest {
//
//	@Autowired
//	private LayersService layersService;
//
//	static {
//		System.setProperty("biocache.config", System.getProperty("user.dir") + "/src/test/resources/biocache-test-config.properties");
//	}
//
//	@Before
//	public void setUp() throws Exception {
//		MockitoAnnotations.initMocks(layersService);
//	}
//
//	@Test
//	public void testLayersStore() {
//		LayersStore ls = new LayersStore(Config.layersServiceUrl());
//
//		try {
//			assertTrue(ls.getFieldIds().size() > 0);
//			assertTrue(ls.getFieldIdsAndDisplayNames().size() > 0);
//
//			String[] fields = new String[]{"aus1", "aus2"};
//			double[][] points = new double[][]{{0, 0}, {134, -22}};
//			CSVReader csv = new CSVReader(ls.sample(fields, points, null));
//			List<String[]> sampleResult = csv.readAll();
//
//			assertTrue(sampleResult.size() == 3); // header, 2 data rows (1 row for each input point)
//			assertTrue(sampleResult.get(0).length == 4); // row content is latitude, longitude, aus1, aus2
//			assertTrue(sampleResult.get(1).length == 4); // row content is latitude, longitude, aus1, aus2
//			assertTrue(sampleResult.get(2).length == 4); // row content is latitude, longitude, aus1, aus2
//
//		} catch (Exception e) {
//			e.printStackTrace();
//			assertTrue(false);
//		}
//	}
//
//	@Test
//	public void testAlaLayersService() {
//		assertTrue(layersService.getLayerNameMap().size() > 0);
//		assertTrue(layersService.getDistributionsCount("urn:lsid:biodiversity.org.au:afd.taxon:8dd6937f-1032-43d3-a75c-da404515743b") > 0);
//		assertTrue(layersService.getChecklistsCount("urn:lsid:biodiversity.org.au:afd.taxon:e617b4ec-e848-4759-bc1f-60d085f00fe5") > 0);
//		assertTrue(layersService.getTracksCount("1") == 0);
//		assertTrue(layersService.findAnalysisLayerName("aus1", null) != null);
//	}
}
