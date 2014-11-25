package org.mrgeo.junit;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
@RunWith(Categories.class)
@Categories.IncludeCategory(IntegrationTest.class)
@Categories.ExcludeCategory(UnitTest.class)
@org.junit.runners.Suite.SuiteClasses({AllTests.class})
public class IntegrationTestSuite {
}
