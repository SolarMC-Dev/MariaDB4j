/*
 * #%L
 * MariaDB4j
 * %%
 * Copyright (C) 2012 - 2014 Michael Vorburger
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package ch.vorburger.mariadb4j.tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import ch.vorburger.mariadb4j.Util;

/**
 * Test for ClasspathUnpacker.
 * 
 * @author Michael Vorburger
 */
public class ClasspathUnpackerTest {

    private Path newCleanDir(String name) throws IOException {
        Path dir = Path.of("target").resolve("test-unpack").resolve(name);
        Util.deleteRecursively(dir);
        return dir;
    }

    @Test
    public void testClasspathUnpackerFromUniqueClasspath() throws IOException {
        Path toDir = newCleanDir("testUnpack1");
        Util.extractFromClasspathToFile("org/apache/commons/exec", toDir);
        Assert.assertTrue(Files.exists(toDir.resolve("CommandLine.class")));
    }

    @Test(expected = IOException.class)
    @Ignore
    // Not yet implemented... not really important
    public void testClasspathUnpackerFromDuplicateClasspath() throws IOException {
        Path toDir = newCleanDir("testUnpack3");
        Util.extractFromClasspathToFile("META-INF/maven", toDir);
    }

    @Test
    public void testClasspathUnpackerFromFilesystem() throws IOException {
        Path toDir = newCleanDir("testUnpack3");
        int c1 = Util.extractFromClasspathToFile("test", toDir);
        Assert.assertEquals(3, c1);
        Assert.assertTrue(Files.exists(toDir.resolve("a.txt")));
        Assert.assertTrue(Files.exists(toDir.resolve("b.txt")));
        Assert.assertTrue(Files.exists(toDir.resolve("subdir/c.txt")));

        // Now try again - it shouldn't copy anything anymore (optimization)
        int c2 = Util.extractFromClasspathToFile("test", toDir);
        Assert.assertEquals(0, c2);
    }

    @Test(expected = IOException.class)
    public void testClasspathUnpackerPathDoesNotExist() throws IOException {
        Path toDir = newCleanDir("testUnpack4");
        Util.extractFromClasspathToFile("does/not/exist", toDir);
    }

    @Test(expected = IOException.class)
    public void testClasspathUnpackerPackageExistsButIsEmpty() throws IOException {
        Path toDir = newCleanDir("testUnpack4");
        Util.extractFromClasspathToFile("test/empty", toDir);
    }

}
