namespace DurableTaskSamples.Common.WorkItems
{
    using System.Net;
    using System.Net.Mail;
    using DurableTask;

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
