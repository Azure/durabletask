//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Text.RegularExpressions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask.ServiceBus.Tracking;

    [TestClass]
    public class BlobStorageClientHelperTest
    {
        [TestMethod]
        public void IsContainerExpiredTest()
        {
            Assert.AreEqual("ab-cd", BlobStorageClientHelper.BuildContainerName("ab", "cd"));

            Assert.IsTrue(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20100101", DateTime.UtcNow));
            Assert.IsFalse(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-session-20990101", DateTime.UtcNow));

            DateTime dateTime = new DateTime(2015, 05, 17);
            Assert.IsTrue(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20150516", dateTime));
            Assert.IsFalse(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20150517", dateTime));
            Assert.IsFalse(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20150518", dateTime));
            Assert.IsTrue(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20140518", dateTime));

            // invalid containers are ignored
            Assert.IsFalse(BlobStorageClientHelper.IsContainerExpired("invalidContainerName", DateTime.UtcNow));
            Assert.IsFalse(BlobStorageClientHelper.IsContainerExpired("hubname-dtfx-message-20146789", DateTime.UtcNow));
        }

        [TestMethod]
        public void BuildMessageBlobKeyTest()
        {
            string instanceId = "aa";
            string executionId = "bb";
            DateTime messageFireTime = new DateTime(2015, 05, 17);
            string key = BlobStorageClientHelper.BuildMessageBlobKey(instanceId, executionId, messageFireTime);
            Regex regex = new Regex(@"message-20150517|aa/bb/\w{32}$");
            Assert.IsTrue(regex.Match(key).Success);

            key = BlobStorageClientHelper.BuildMessageBlobKey(instanceId, executionId, DateTime.MinValue);
            regex = new Regex(@"message-\d{8}|aa/bb/\w{32}$");
            Assert.IsTrue(regex.Match(key).Success);
        }

        [TestMethod]
        public void BuildSessionBlobKeyTest()
        {
            string sessionId = "abc";
            string key = BlobStorageClientHelper.BuildSessionBlobKey(sessionId);
            Regex regex = new Regex(@"^session-\d{8}|abc/\w{32}$");
            Assert.IsTrue(regex.Match(key).Success);
        }

        [TestMethod]
        public void BuildContainerNamePrefixTest()
        {
            string hubName = "HubName";
            string containerNamePrefix = BlobStorageClientHelper.BuildContainerNamePrefix(hubName);
            Assert.AreEqual("hubname-dtfx", containerNamePrefix);
        }

        [TestMethod]
        public void ParseKeyTest()
        {
            string key = "message-20100319|aa/bb/cc";
            string containerSuffix;
            string blobName;
            BlobStorageClientHelper.ParseKey(key, out containerSuffix, out blobName);

            Assert.AreEqual("message-20100319", containerSuffix);
            Assert.AreEqual("aa/bb/cc", blobName);

            try
            {
                BlobStorageClientHelper.ParseKey("invalidKey", out containerSuffix, out blobName);
                Assert.Fail("ArgumentException must be thrown");
            }
            catch (ArgumentException e)
            {
                Assert.IsTrue(e.Message.Contains("key"), "Exception must contain key.");
            }

            try
            {
                // invalid container name suffix: only lower case letters and numbers are allowed
                BlobStorageClientHelper.ParseKey("Message-20100319|aa/bb/cc", out containerSuffix, out blobName);
                Assert.Fail("ArgumentException must be thrown");
            }
            catch (ArgumentException e)
            {
                Assert.IsTrue(e.Message.Contains("Message-20100319"), "Exception must contain the invalid container name suffix.");
            }
        }
    }
}
