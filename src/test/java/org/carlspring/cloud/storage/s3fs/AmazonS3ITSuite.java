package org.carlspring.cloud.storage.s3fs;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * @author carlspring
 */
@RunWith(JUnitPlatform.class)
@IncludeTags({ "s3-integration-test" })
@SelectPackages({ "org.carlspring.cloud.storage.s3fs" })
public class AmazonS3ITSuite
{
}
