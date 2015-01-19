namespace DurableTaskSamples.Greetings
{
    using System;
    using System.Windows.Forms;
    using DurableTask;

    public sealed class GetUserTask : TaskActivity<string, string>
    {
        public GetUserTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string input)
        {
            GetUserName userNamedialog = new GetUserName();
            Console.WriteLine("Waiting for user to enter name...");
            string user = "";
            DialogResult dialogResult = userNamedialog.ShowDialog();
            if (dialogResult == DialogResult.OK)
            {
                user = userNamedialog.UserName;
            }
            Console.WriteLine("User Name Entered: " + user);

            return user;
        }
    }

}