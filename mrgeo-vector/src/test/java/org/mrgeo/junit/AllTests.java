package org.mrgeo.junit;

import org.junit.extensions.cpsuite.ClasspathSuite;
import org.junit.extensions.cpsuite.ClasspathSuite.SuiteTypes;
import org.junit.runner.RunWith;

import static org.junit.extensions.cpsuite.SuiteType.TEST_CLASSES;

@RunWith(ClasspathSuite.class)
@SuiteTypes({TEST_CLASSES})
public class AllTests 
{
}
