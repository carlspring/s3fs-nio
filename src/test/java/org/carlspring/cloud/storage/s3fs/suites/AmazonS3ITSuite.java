package org.carlspring.cloud.storage.s3fs.suites;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * @author carlspring
 */
@RunWith(JUnitPlatform.class)
@IncludeTags({ "s" })
@SelectPackages("org.carlspring.cloud.storage.s3fs.path")
public class AmazonS3ITSuite
{
}
