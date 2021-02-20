package org.carlspring.cloud.storage.s3fs;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3AwsRegionProviderChainTest
{

    @Test
    void firstProviderInChainGivesSystemSettingsRegionInformation()
    {
        //given
        final String expectedRegion = "some-region-string";
        System.setProperty(SdkSystemSetting.AWS_REGION.property(), expectedRegion);

        //when
        final AwsRegionProviderChain chain = S3AwsRegionProviderChain.builder().build();

        //then
        assertEquals(expectedRegion, chain.getRegion().toString());
    }

    @Test
    void whenFirstProviderInChainHasNotSystemSettingsRegionPropertyItShouldThrowAnException()
    {
        //given
        // Region is not set for AWS Region system setting, so it throws an exception and jumps to next provider.
        System.setProperty(SdkSystemSetting.AWS_REGION.property(), "");

        //when
        final AwsRegionProviderChain chain = S3AwsRegionProviderChain.builder().build();

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(SdkClientException.class, chain::getRegion);

        //then
        assertNotNull(exception);
        assertTrue(exception.getMessage().startsWith("Unable to load region from any of the providers in the chain"));
        assertTrue(exception.getMessage().contains("region must not be blank or empty."));
    }

    @Test
    void secondProviderInChainGivesAwsProfileRegionInformation()
            throws URISyntaxException
    {
        //given
        final String expectedRegion = "eu-central-1";

        // Region is not set for AWS Region system setting, so it jumps to next provider.
        System.setProperty(SdkSystemSetting.AWS_REGION.property(), "");

        // AWS profile data
        final String profileFileName = "config-test";
        final Path profilePath = Paths.get(ClassLoader.getSystemResource(profileFileName).toURI());
        final Supplier<ProfileFile> profileFile = () -> ProfileFile.builder()
                                                                   .type(ProfileFile.Type.CONFIGURATION)
                                                                   .content(profilePath)
                                                                   .build();
        final String profileName = "test";

        //when
        final AwsRegionProviderChain chain = S3AwsRegionProviderChain.builder()
                                                                     .profileFile(profileFile)
                                                                     .profileName(profileName)
                                                                     .build();

        //then
        assertEquals(expectedRegion, chain.getRegion().toString());
    }

    @Test
    void whenSecondProviderInChainHasAwsProfileWithoutRegionItShouldThrowAnException()
            throws URISyntaxException
    {
        //given
        // Region is not set for AWS Region system setting, so it jumps to next provider.
        System.setProperty(SdkSystemSetting.AWS_REGION.property(), "");

        // AWS profile data
        final String profileFileName = "config-test";
        final Path profilePath = Paths.get(ClassLoader.getSystemResource(profileFileName).toURI());
        final Supplier<ProfileFile> profileFile = () -> ProfileFile.builder()
                                                                   .type(ProfileFile.Type.CONFIGURATION)
                                                                   .content(profilePath)
                                                                   .build();
        final String profileName = "test-no-region";

        final S3AwsRegionProviderChain chain = S3AwsRegionProviderChain.builder()
                                                                       .profileFile(profileFile)
                                                                       .profileName(profileName)
                                                                       .build();

        //when
        // We're expecting an exception here to be thrown
        final Exception exception = assertThrows(SdkClientException.class, chain::getRegion);

        //then
        assertNotNull(exception);
        assertTrue(exception.getMessage().startsWith("Unable to load region from any of the providers in the chain"));
        assertTrue(exception.getMessage().contains("region must not be blank or empty."));
        assertTrue(exception.getMessage().contains("No region provided in profile: " + profileName));
    }


}
