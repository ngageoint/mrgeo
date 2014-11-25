package org.mrgeo.junit;

import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;

@RunWith(Categories.class)
@Categories.ExcludeCategory(org.mrgeo.junit.IntegrationTest.class)
@Categories.IncludeCategory(org.mrgeo.junit.UnitTest.class)
@org.junit.runners.Suite.SuiteClasses({AllTests.class})
public class UnitTestSuite {
}
