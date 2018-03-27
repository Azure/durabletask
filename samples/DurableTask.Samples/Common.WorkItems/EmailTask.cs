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

namespace DurableTask.Samples.Common.WorkItems
{
    using System.Net;
    using System.Net.Mail;
    using DurableTask.Core;

    public sealed class EmailInput
    {
        public string To;
        public string ToAddress;
        public string Subject;
        public string Body;
    }

    public sealed class EmailTask : TaskActivity<EmailInput, object>
    {
        private static MailAddress FromAddress = new MailAddress("azuresbtest@outlook.com", "Service Bus Task Mailer");
        private const string FromPassword = "Broken!12";

        public EmailTask()
        {
        }

        protected override object Execute(TaskContext context, EmailInput input)
        {
            var toAddress = new MailAddress(input.ToAddress, input.To);

            var smtp = new SmtpClient
                {
                    Host = "smtp.live.com",
                    Port = 587,
                    EnableSsl = true,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = false,
                    Credentials = new NetworkCredential(FromAddress.Address, FromPassword)
                };

            using (var message = new MailMessage(FromAddress, toAddress)
                {
                    Subject = input.Subject,
                    Body = input.Body
                })
            {
                smtp.Send(message);
            }
            return null;
        }

    }
}
