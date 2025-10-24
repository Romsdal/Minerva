using System;
using MemoryPack;
using ProtoBuf;

namespace Minerva.DB_Server.Network.Protos;

public interface TPCCItem
{
    public void Patch(TPCCItem other);
    public bool IsPatch { get; }
}

[MemoryPackable]
[ProtoContract]
public partial class Warehouse : TPCCItem
{

    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long W_ID { get; set; }

    [ProtoMember(3)]
    public string W_NAME { get; set; }

    [ProtoMember(4)]
    public string W_STREET_1 { get; set; }

    [ProtoMember(5)]
    public string W_STREET_2 { get; set; }

    [ProtoMember(6)]
    public string W_CITY { get; set; }

    [ProtoMember(7)]
    public string W_STATE { get; set; }

    [ProtoMember(8)]
    public string W_ZIP { get; set; }

    [ProtoMember(9)]
    public double W_TAX { get; set; }

    [ProtoMember(10)]
    public double W_YTD { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not Warehouse o) return;
        if (!o.Is_Patch) return;

        if (o.W_NAME != null) W_NAME = o.W_NAME;
        if (o.W_STREET_1 != null) W_STREET_1 = o.W_STREET_1;
        if (o.W_STREET_2 != null) W_STREET_2 = o.W_STREET_2;
        if (o.W_CITY != null) W_CITY = o.W_CITY;
        if (o.W_STATE != null) W_STATE = o.W_STATE;
        if (o.W_ZIP != null) W_ZIP = o.W_ZIP;
        if (o.W_TAX != W_TAX) W_TAX = o.W_TAX;
        if (o.W_YTD != W_YTD) W_YTD = o.W_YTD;
    }

    public bool IsPatch { get => Is_Patch; }
}

[MemoryPackable]
[ProtoContract]
public partial class District : TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long D_ID { get; set; }

    [ProtoMember(3)]
    public long D_W_ID { get; set; }

    [ProtoMember(4)]
    public string D_NAME { get; set; }

    [ProtoMember(5)]
    public string D_STREET_1 { get; set; }

    [ProtoMember(6)]
    public string D_STREET_2 { get; set; }

    [ProtoMember(7)]
    public string D_CITY { get; set; }

    [ProtoMember(8)]
    public string D_STATE { get; set; }

    [ProtoMember(9)]
    public string D_ZIP { get; set; }

    [ProtoMember(10)]
    public double D_TAX { get; set; }

    [ProtoMember(11)]
    public double D_YTD { get; set; }

    [ProtoMember(12)]
    public long D_NEXT_O_ID { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not District o) return;
        if (!o.Is_Patch) return;

        if (o.D_NAME != null) D_NAME = o.D_NAME;
        if (o.D_STREET_1 != null) D_STREET_1 = o.D_STREET_1;
        if (o.D_STREET_2 != null) D_STREET_2 = o.D_STREET_2;
        if (o.D_CITY != null) D_CITY = o.D_CITY;
        if (o.D_STATE != null) D_STATE = o.D_STATE;
        if (o.D_ZIP != null) D_ZIP = o.D_ZIP;
        if (o.D_TAX != D_TAX) D_TAX = o.D_TAX;
        if (o.D_YTD != D_YTD) D_YTD = o.D_YTD;
        if (o.D_NEXT_O_ID != D_NEXT_O_ID) D_NEXT_O_ID = o.D_NEXT_O_ID;
    }

    public bool IsPatch { get => Is_Patch; }
}

[MemoryPackable]
[ProtoContract]
public partial class Customer: TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long C_ID { get; set; }

    [ProtoMember(3)]
    public long C_D_ID { get; set; }

    [ProtoMember(4)]
    public long C_W_ID { get; set; }

    [ProtoMember(5)]
    public string C_FIRST { get; set; }

    [ProtoMember(6)]
    public string C_MIDDLE { get; set; }

    [ProtoMember(7)]
    public string C_LAST { get; set; }
    
    [ProtoMember(8)]
    public string C_STREET_1 { get; set; }

    [ProtoMember(9)]
    public string C_STREET_2 { get; set; }

    [ProtoMember(10)]
    public string C_CITY { get; set; }

    [ProtoMember(11)]
    public string C_STATE { get; set; }

    [ProtoMember(12)]
    public string C_ZIP { get; set; }

    [ProtoMember(13)]
    public string C_PHONE { get; set; }

    [ProtoMember(14)]
    public long C_SINCE { get; set; }

    [ProtoMember(15)]
    public string C_CREDIT { get; set; }

    [ProtoMember(16)]
    public long C_CREDIT_LIM { get; set; }

    [ProtoMember(17)]
    public double C_DISCOUNT { get; set; }

    [ProtoMember(18)]
    public double C_BALANCE { get; set; }

    [ProtoMember(19)]
    public double C_YTD_PAYMENT { get; set; }

    [ProtoMember(20)]
    public long C_PAYMENT_CNT { get; set; }
    
    [ProtoMember(21)]
    public int C_DELIVERY_CNT { get; set; }
    
    [ProtoMember(22)]
    public string C_DATA { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not Customer o) return;
        if (!o.Is_Patch) return;

        if (o.C_FIRST != null) C_FIRST = o.C_FIRST;
        if (o.C_MIDDLE != null) C_MIDDLE = o.C_MIDDLE;
        if (o.C_LAST != null) C_LAST = o.C_LAST;
        if (o.C_STREET_1 != null) C_STREET_1 = o.C_STREET_1;
        if (o.C_STREET_2 != null) C_STREET_2 = o.C_STREET_2;
        if (o.C_CITY != null) C_CITY = o.C_CITY;
        if (o.C_STATE != null) C_STATE = o.C_STATE;
        if (o.C_ZIP != null) C_ZIP = o.C_ZIP;
        if (o.C_PHONE != null) C_PHONE = o.C_PHONE;
        if (o.C_SINCE != C_SINCE) C_SINCE = o.C_SINCE;
        if (o.C_CREDIT != null) C_CREDIT = o.C_CREDIT;
        if (o.C_CREDIT_LIM != C_CREDIT_LIM) C_CREDIT_LIM = o.C_CREDIT_LIM;
        if (o.C_DISCOUNT != C_DISCOUNT) C_DISCOUNT = o.C_DISCOUNT;
        if (o.C_BALANCE != C_BALANCE) C_BALANCE = o.C_BALANCE;
        if (o.C_YTD_PAYMENT != C_YTD_PAYMENT) C_YTD_PAYMENT = o.C_YTD_PAYMENT;
        if (o.C_PAYMENT_CNT != C_PAYMENT_CNT) C_PAYMENT_CNT = o.C_PAYMENT_CNT;
        if (o.C_DELIVERY_CNT != C_DELIVERY_CNT) C_DELIVERY_CNT = o.C_DELIVERY_CNT;
        if (o.C_DATA != null) C_DATA = o.C_DATA;
    }

    public bool IsPatch { get => Is_Patch; }

}

[MemoryPackable]
[ProtoContract]
public partial class Item : TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long I_ID { get; set; }

    [ProtoMember(3)]
    public long I_IM_ID { get; set; }

    [ProtoMember(4)]
    public string I_NAME { get; set; }

    [ProtoMember(5)]
    public double I_PRICE { get; set; }

    [ProtoMember(6)]
    public string I_DATA { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not Item o) return;
        if (!o.Is_Patch) return;

        if (o.I_IM_ID != I_IM_ID) I_IM_ID = o.I_IM_ID;
        if (o.I_NAME != null) I_NAME = o.I_NAME;
        if (o.I_PRICE != I_PRICE) I_PRICE = o.I_PRICE;
        if (o.I_DATA != null) I_DATA = o.I_DATA;
    }

    public bool IsPatch { get => Is_Patch; }

}

[MemoryPackable]
[ProtoContract]
public partial class Stock : TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long S_I_ID { get; set; }

    [ProtoMember(3)]
    public long S_W_ID { get; set; }

    [ProtoMember(4)]
    public long S_QUANTITY { get; set; }

    [ProtoMember(5)]
    public string S_DIST_01 { get; set; }

    [ProtoMember(6)]
    public string S_DIST_02 { get; set; }

    [ProtoMember(7)]
    public string S_DIST_03 { get; set; }

    [ProtoMember(8)]
    public string S_DIST_04 { get; set; }
    
    [ProtoMember(9)]
    public string S_DIST_05 { get; set; }

    [ProtoMember(10)]
    public string S_DIST_06 { get; set; }

    [ProtoMember(11)]
    public string S_DIST_07 { get; set; }
    
    [ProtoMember(12)]
    public string S_DIST_08 { get; set; }
    
    [ProtoMember(13)]
    public string S_DIST_09 { get; set; }
    
    [ProtoMember(14)]
    public string S_DIST_10 { get; set; }
    
    [ProtoMember(15)]
    public long S_YTD { get; set; }
    
    [ProtoMember(16)]
    public long S_ORDER_CNT { get; set; }
    
    [ProtoMember(17)]
    public long S_REMOTE_CNT { get; set; }
    
    [ProtoMember(18)]
    public string S_DATA { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not Stock o) return;
        if (!o.Is_Patch) return;

        if (o.S_QUANTITY != S_QUANTITY) S_QUANTITY = o.S_QUANTITY;
        if (o.S_DIST_01 != null) S_DIST_01 = o.S_DIST_01;
        if (o.S_DIST_02 != null) S_DIST_02 = o.S_DIST_02;
        if (o.S_DIST_03 != null) S_DIST_03 = o.S_DIST_03;
        if (o.S_DIST_04 != null) S_DIST_04 = o.S_DIST_04;
        if (o.S_DIST_05 != null) S_DIST_05 = o.S_DIST_05;
        if (o.S_DIST_06 != null) S_DIST_06 = o.S_DIST_06;
        if (o.S_DIST_07 != null) S_DIST_07 = o.S_DIST_07;
        if (o.S_DIST_08 != null) S_DIST_08 = o.S_DIST_08;
        if (o.S_DIST_09 != null) S_DIST_09 = o.S_DIST_09;
        if (o.S_DIST_10 != null) S_DIST_10 = o.S_DIST_10;
        if (o.S_YTD != S_YTD) S_YTD = o.S_YTD;
        if (o.S_ORDER_CNT != S_ORDER_CNT) S_ORDER_CNT = o.S_ORDER_CNT;
        if (o.S_REMOTE_CNT != S_REMOTE_CNT) S_REMOTE_CNT = o.S_REMOTE_CNT;
        if (o.S_DATA != null) S_DATA = o.S_DATA;
    }

    public bool IsPatch { get => Is_Patch; }
}

[MemoryPackable]
[ProtoContract]
public partial class History : TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long H_C_ID { get; set; }

    [ProtoMember(3)]
    public long H_C_D_ID { get; set; }

    [ProtoMember(4)]
    public long H_C_W_ID { get; set; }

    [ProtoMember(5)]
    public long H_D_ID { get; set; }

    [ProtoMember(6)]
    public long H_W_ID { get; set; }
    
    [ProtoMember(7)]
    public long H_DATE { get; set; }

    [ProtoMember(8)]
    public double H_AMOUNT { get; set; }

    [ProtoMember(9)]
    public string H_DATA { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not History o) return;
        if (!o.Is_Patch) return;

        if (o.H_DATE != H_DATE) H_DATE = o.H_DATE;
        if (o.H_AMOUNT != H_AMOUNT) H_AMOUNT = o.H_AMOUNT;
        if (o.H_DATA != null) H_DATA = o.H_DATA;
    }

    public bool IsPatch { get => Is_Patch; }
}

[MemoryPackable]
[ProtoContract]
public partial class NewOrder : TPCCItem
{
    [ProtoMember(1)]
    public long NO_O_ID { get; set; }
    
    [ProtoMember(2)]
    public long NO_D_ID { get; set; }
    
    [ProtoMember(3)]
    public long NO_W_ID { get; set; }

    public void Patch(TPCCItem other)
    {
        throw new NotImplementedException();
    }

    public bool IsPatch { get => false; }
}

[MemoryPackable]
[ProtoContract]
public partial class Order : TPCCItem
{
    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long O_ID { get; set; }

    [ProtoMember(3)]
    public long O_C_ID { get; set; }

    [ProtoMember(4)]
    public long O_D_ID { get; set; }

    [ProtoMember(5)]
    public long O_W_ID { get; set; }

    [ProtoMember(6)]
    public long O_ENTRY_D { get; set; }

    [ProtoMember(7)]
    public long O_CARRIER_ID { get; set; }

    [ProtoMember(8)]
    public long O_OL_CNT { get; set; }

    [ProtoMember(9)]
    public bool O_ALL_LOCAL { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not Order o) return;
        if (!o.Is_Patch) return;

        if (o.O_ENTRY_D != O_ENTRY_D) O_ENTRY_D = o.O_ENTRY_D;
        if (o.O_CARRIER_ID != O_CARRIER_ID) O_CARRIER_ID = o.O_CARRIER_ID;
        if (o.O_OL_CNT != O_OL_CNT) O_OL_CNT = o.O_OL_CNT;
        if (o.O_ALL_LOCAL != O_ALL_LOCAL) O_ALL_LOCAL = o.O_ALL_LOCAL;
    }

    public bool IsPatch { get => Is_Patch; }
}

[MemoryPackable]
[ProtoContract]
public partial class OrderLine : TPCCItem
{


    [ProtoMember(1)]
    public bool Is_Patch { get; set; } = false;

    [ProtoMember(2)]
    public long OL_O_ID { get; set; }

    [ProtoMember(3)]
    public long OL_D_ID { get; set; }

    [ProtoMember(4)]
    public long OL_W_ID { get; set; }

    [ProtoMember(5)]
    public long OL_NUMBER { get; set; }

    [ProtoMember(6)]
    public long OL_I_ID { get; set; }

    [ProtoMember(7)]
    public long OL_SUPPLY_W_ID { get; set; }

    [ProtoMember(8)]
    public long OL_DELIVERY_D { get; set; }

    [ProtoMember(9)]
    public long OL_QUANTITY { get; set; }

    [ProtoMember(10)]
    public double OL_AMOUNT { get; set; }

    [ProtoMember(11)]
    public string OL_DIST_INFO { get; set; }

    public void Patch(TPCCItem other)
    {
        if (other is not OrderLine o) return;
        if (!o.Is_Patch) return;

        if (o.OL_I_ID != OL_I_ID) OL_I_ID = o.OL_I_ID;
        if (o.OL_SUPPLY_W_ID != OL_SUPPLY_W_ID) OL_SUPPLY_W_ID = o.OL_SUPPLY_W_ID;
        if (o.OL_DELIVERY_D != OL_DELIVERY_D) OL_DELIVERY_D = o.OL_DELIVERY_D;
        if (o.OL_QUANTITY != OL_QUANTITY) OL_QUANTITY = o.OL_QUANTITY;
        if (o.OL_AMOUNT != OL_AMOUNT) OL_AMOUNT = o.OL_AMOUNT;
        if (o.OL_DIST_INFO != null) OL_DIST_INFO = o.OL_DIST_INFO;
    }

    public bool IsPatch { get => Is_Patch; }
}