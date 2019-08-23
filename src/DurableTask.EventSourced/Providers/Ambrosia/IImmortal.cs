using System;
using System.Collections.Generic;
using System.Text;
//using Ambrosia;

namespace DurableTask.EventSourced.Ambrosia
{
    interface IImmortal
    {
        void OrderedEvent(Event evt);

        //[ImpulseHandler]
        void ImpulseEvent(Event evt);
    }
}
